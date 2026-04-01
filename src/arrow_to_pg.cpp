/*
 * arrow_to_pg.cpp
 *
 * Convert Arrow C Data Interface arrays to openGauss Datums.
 *
 * This file handles the low-level conversion from Arrow column buffers
 * (as exported via the Arrow C Data Interface) to PostgreSQL Datum values
 * suitable for filling a TupleTableSlot.
 *
 * Type mapping follows pglance's type_mapping.rs and convert.rs, translated
 * to C operating directly on raw Arrow buffer pointers.
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "catalog/pg_type.h"
#include "fmgr.h"

#include "arrow_c_data.h"
#include "lance_fdw.h"

/*
 * Epoch difference between Unix epoch (1970-01-01) and PostgreSQL epoch
 * (2000-01-01) in various units.
 */
#define UNIX_TO_PG_EPOCH_DAYS      10957
#define UNIX_TO_PG_EPOCH_USEC      INT64CONST(946684800000000)  /* 10957 days in microseconds */
#define UNIX_TO_PG_EPOCH_SEC       INT64CONST(946684800)
#define UNIX_TO_PG_EPOCH_MSEC      INT64CONST(946684800000)

/*
 * Check if a value is NULL in an Arrow array by inspecting the validity bitmap.
 * Arrow stores validity in buffers[0] as a packed bit array.
 * Returns true if the value is NULL.
 */
static inline bool
arrow_is_null(const struct ArrowArray *array, int64_t row)
{
    const uint8_t *validity;

    /* null_count == 0 means no nulls at all */
    if (array->null_count == 0)
        return false;

    /* No validity bitmap means all values are valid */
    if (array->buffers[0] == NULL)
        return false;

    validity = (const uint8_t *) array->buffers[0];
    row += array->offset;
    return !(validity[row / 8] & (1 << (row % 8)));
}

/*
 * ============================================================
 * Arrow format string parsing helpers
 * ============================================================
 *
 * Arrow C Data Interface uses format strings to describe types:
 *   "b"  = bool
 *   "c"  = int8
 *   "C"  = uint8
 *   "s"  = int16
 *   "S"  = uint16
 *   "i"  = int32
 *   "I"  = uint32
 *   "l"  = int64
 *   "L"  = uint64
 *   "e"  = float16
 *   "f"  = float32
 *   "g"  = float64
 *   "u"  = utf8
 *   "U"  = large_utf8
 *   "z"  = binary
 *   "Z"  = large_binary
 *   "tdD" = date32 (days)
 *   "tdm" = date64 (milliseconds)
 *   "tss:" = timestamp seconds
 *   "tsm:" = timestamp milliseconds
 *   "tsu:" = timestamp microseconds
 *   "tsn:" = timestamp nanoseconds
 *   "d:P,S" = decimal128(P,S)
 *   "+s"  = struct
 *   "+l"  = list
 *   "+L"  = large_list
 *   "+w:N" = fixed_size_list(N)
 *   "w:N" = fixed_size_binary(N)
 */

/*
 * Convert an Arrow value at the given row index to a PG Datum.
 *
 * This is the main dispatch function. It inspects the Arrow format string
 * and calls the appropriate conversion routine.
 */
Datum
arrow_value_to_datum(const struct ArrowArray *array,
                     const struct ArrowSchema *schema,
                     int64_t row,
                     Oid pg_type_oid,
                     bool *isnull)
{
    const char *fmt;

    *isnull = false;

    /* Check NULL first */
    if (arrow_is_null(array, row))
    {
        *isnull = true;
        return (Datum) 0;
    }

    fmt = schema->format;
    if (fmt == NULL)
    {
        elog(ERROR, "lance_fdw: NULL format string in ArrowSchema");
        *isnull = true;
        return (Datum) 0;
    }

    /*
     * Guard: buffers pointer must exist.
     * Note: n_buffers varies by type:
     *   - Scalar types: n_buffers >= 2 (validity + data)
     *   - Struct/List: n_buffers == 1 (validity only, data in children)
     *   - Dictionary: index array has n_buffers >= 2
     * We only check buffers != NULL here; individual type handlers
     * access specific buffer indices they need.
     */
    if (array->buffers == NULL && fmt[0] != '+')
    {
        *isnull = true;
        return (Datum) 0;
    }

    /*
     * Dictionary-encoded types are unpacked to plain arrays in Rust
     * (unpack_dictionaries in arrow_export.rs) before FFI export,
     * so we never see dictionary arrays here. If we do (shouldn't happen),
     * fall through to the normal type dispatch below.
     */

    /*
     * Boolean: format "b"
     * Stored as packed bits in buffers[1].
     */
    if (fmt[0] == 'b' && fmt[1] == '\0')
    {
        const uint8_t *data = (const uint8_t *) array->buffers[1];
        int64_t idx = row + array->offset;
        bool val = (data[idx / 8] >> (idx % 8)) & 1;
        return BoolGetDatum(val);
    }

    /*
     * Integer types
     */
    /* int8 */
    if (fmt[0] == 'c' && fmt[1] == '\0')
    {
        const int8_t *data = (const int8_t *) array->buffers[1];
        return Int16GetDatum((int16) data[row + array->offset]);
    }
    /* uint8 */
    if (fmt[0] == 'C' && fmt[1] == '\0')
    {
        const uint8_t *data = (const uint8_t *) array->buffers[1];
        return Int16GetDatum((int16) data[row + array->offset]);
    }
    /* int16 */
    if (fmt[0] == 's' && fmt[1] == '\0')
    {
        const int16_t *data = (const int16_t *) array->buffers[1];
        return Int16GetDatum(data[row + array->offset]);
    }
    /* uint16 */
    if (fmt[0] == 'S' && fmt[1] == '\0')
    {
        const uint16_t *data = (const uint16_t *) array->buffers[1];
        return Int32GetDatum((int32) data[row + array->offset]);
    }
    /* int32 */
    if (fmt[0] == 'i' && fmt[1] == '\0')
    {
        const int32_t *data = (const int32_t *) array->buffers[1];
        return Int32GetDatum(data[row + array->offset]);
    }
    /* uint32 */
    if (fmt[0] == 'I' && fmt[1] == '\0')
    {
        const uint32_t *data = (const uint32_t *) array->buffers[1];
        return Int64GetDatum((int64) data[row + array->offset]);
    }
    /* int64 */
    if (fmt[0] == 'l' && fmt[1] == '\0')
    {
        const int64_t *data = (const int64_t *) array->buffers[1];
        return Int64GetDatum(data[row + array->offset]);
    }
    /* uint64 */
    if (fmt[0] == 'L' && fmt[1] == '\0')
    {
        const uint64_t *data = (const uint64_t *) array->buffers[1];
        /* Treat as int64 - may overflow for very large values */
        return Int64GetDatum((int64) data[row + array->offset]);
    }

    /*
     * Float types
     */
    /* float16 - stored as uint16, convert to float32 for PG */
    if (fmt[0] == 'e' && fmt[1] == '\0')
    {
        const uint16_t *data = (const uint16_t *) array->buffers[1];
        uint16_t half = data[row + array->offset];
        /* Basic half-float to float conversion */
        uint32_t sign = (half >> 15) & 0x1;
        uint32_t exp = (half >> 10) & 0x1F;
        uint32_t mant = half & 0x3FF;
        float val;
        if (exp == 0)
        {
            if (mant == 0)
                val = sign ? -0.0f : 0.0f;
            else
                val = (sign ? -1.0f : 1.0f) * ldexpf((float) mant, -24);
        }
        else if (exp == 31)
        {
            val = mant ? NAN : (sign ? -INFINITY : INFINITY);
        }
        else
        {
            val = (sign ? -1.0f : 1.0f) * ldexpf(1.0f + (float) mant / 1024.0f, (int) exp - 15);
        }
        return Float4GetDatum(val);
    }
    /* float32 */
    if (fmt[0] == 'f' && fmt[1] == '\0')
    {
        const float *data = (const float *) array->buffers[1];
        return Float4GetDatum(data[row + array->offset]);
    }
    /* float64 */
    if (fmt[0] == 'g' && fmt[1] == '\0')
    {
        const double *data = (const double *) array->buffers[1];
        return Float8GetDatum(data[row + array->offset]);
    }

    /*
     * String types: utf8 "u" and large_utf8 "U"
     * Layout: offsets in buffers[1], data in buffers[2]
     */
    if (fmt[0] == 'u' && fmt[1] == '\0')
    {
        const int32_t *offsets = (const int32_t *) array->buffers[1];
        const char *data = (const char *) array->buffers[2];
        int64_t idx = row + array->offset;
        int32_t start = offsets[idx];
        int32_t len = offsets[idx + 1] - start;
        return PointerGetDatum(cstring_to_text_with_len(data + start, len));
    }
    if (fmt[0] == 'U' && fmt[1] == '\0')
    {
        const int64_t *offsets = (const int64_t *) array->buffers[1];
        const char *data = (const char *) array->buffers[2];
        int64_t idx = row + array->offset;
        int64_t start = offsets[idx];
        int32_t len = (int32_t)(offsets[idx + 1] - start);
        return PointerGetDatum(cstring_to_text_with_len(data + start, len));
    }

    /*
     * Binary types: binary "z" and large_binary "Z"
     */
    if (fmt[0] == 'z' && fmt[1] == '\0')
    {
        const int32_t *offsets = (const int32_t *) array->buffers[1];
        const char *data = (const char *) array->buffers[2];
        int64_t idx = row + array->offset;
        int32_t start = offsets[idx];
        int32_t len = offsets[idx + 1] - start;
        bytea *result = (bytea *) palloc(VARHDRSZ + len);
        SET_VARSIZE(result, VARHDRSZ + len);
        memcpy(VARDATA(result), data + start, len);
        return PointerGetDatum(result);
    }
    if (fmt[0] == 'Z' && fmt[1] == '\0')
    {
        const int64_t *offsets = (const int64_t *) array->buffers[1];
        const char *data = (const char *) array->buffers[2];
        int64_t idx = row + array->offset;
        int64_t start = offsets[idx];
        int32_t len = (int32_t)(offsets[idx + 1] - start);
        bytea *result = (bytea *) palloc(VARHDRSZ + len);
        SET_VARSIZE(result, VARHDRSZ + len);
        memcpy(VARDATA(result), data + start, len);
        return PointerGetDatum(result);
    }

    /*
     * Fixed-size binary "w:N"
     */
    if (fmt[0] == 'w' && fmt[1] == ':')
    {
        int byte_width = atoi(fmt + 2);
        const char *data = (const char *) array->buffers[1];
        int64_t idx = row + array->offset;
        bytea *result = (bytea *) palloc(VARHDRSZ + byte_width);
        SET_VARSIZE(result, VARHDRSZ + byte_width);
        memcpy(VARDATA(result), data + idx * byte_width, byte_width);
        return PointerGetDatum(result);
    }

    /*
     * Date types
     */
    /* date32 (days since Unix epoch) */
    if (fmt[0] == 't' && fmt[1] == 'd' && fmt[2] == 'D' && fmt[3] == '\0')
    {
        const int32_t *data = (const int32_t *) array->buffers[1];
        int32_t unix_days = data[row + array->offset];
        /* Convert from Unix epoch to PG epoch */
        DateADT pg_date = unix_days - UNIX_TO_PG_EPOCH_DAYS;
        return DateADTGetDatum(pg_date);
    }
    /* date64 (milliseconds since Unix epoch) */
    if (fmt[0] == 't' && fmt[1] == 'd' && fmt[2] == 'm' && fmt[3] == '\0')
    {
        const int64_t *data = (const int64_t *) array->buffers[1];
        int64_t unix_ms = data[row + array->offset];
        /* Convert ms to days, then to PG epoch */
        DateADT pg_date = (int32_t)(unix_ms / 86400000LL) - UNIX_TO_PG_EPOCH_DAYS;
        return DateADTGetDatum(pg_date);
    }

    /*
     * Timestamp types: format "ts{unit}:{tz}"
     *   tss: seconds
     *   tsm: milliseconds
     *   tsu: microseconds
     *   tsn: nanoseconds
     * Timezone after ':' may be empty (no TZ) or a timezone string.
     */
    if (fmt[0] == 't' && fmt[1] == 's' && (fmt[2] == 's' || fmt[2] == 'm' ||
        fmt[2] == 'u' || fmt[2] == 'n') && fmt[3] == ':')
    {
        const int64_t *data = (const int64_t *) array->buffers[1];
        int64_t val = data[row + array->offset];
        int64_t pg_usec;

        switch (fmt[2])
        {
            case 's':   /* seconds -> microseconds */
                pg_usec = val * 1000000LL - UNIX_TO_PG_EPOCH_USEC;
                break;
            case 'm':   /* milliseconds -> microseconds */
                pg_usec = val * 1000LL - UNIX_TO_PG_EPOCH_USEC;
                break;
            case 'u':   /* microseconds */
                pg_usec = val - UNIX_TO_PG_EPOCH_USEC;
                break;
            case 'n':   /* nanoseconds -> microseconds */
                pg_usec = val / 1000LL - UNIX_TO_PG_EPOCH_USEC;
                break;
            default:
                pg_usec = 0;    /* unreachable */
                break;
        }

        /* Check if there's a timezone (format: "tsu:UTC" vs "tsu:") */
        if (fmt[4] != '\0')
        {
            /* timestamptz */
            return TimestampGetDatum((Timestamp) pg_usec);
        }
        else
        {
            /* timestamp without tz */
            return TimestampGetDatum((Timestamp) pg_usec);
        }
    }

    /*
     * Decimal128: format "d:P,S" or "d:P,S,128"
     * Stored as 128-bit little-endian integers in buffers[1].
     * Convert to string, then use numeric_in.
     */
    if (fmt[0] == 'd' && fmt[1] == ':')
    {
        const uint8_t *data = (const uint8_t *) array->buffers[1];
        int64_t idx = row + array->offset;
        /* Parse precision and scale from format string */
        int precision = 0, scale = 0;
        const char *p = fmt + 2;
        while (*p >= '0' && *p <= '9')
            precision = precision * 10 + (*p++ - '0');
        if (*p == ',')
            p++;
        while (*p >= '0' && *p <= '9')
            scale = scale * 10 + (*p++ - '0');

        /* Read 128-bit little-endian integer */
        const uint8_t *bytes = data + idx * 16;
        uint64_t lo = 0, hi = 0;
        int k;
        for (k = 7; k >= 0; k--)
            lo = (lo << 8) | bytes[k];
        for (k = 15; k >= 8; k--)
            hi = (hi << 8) | bytes[k];

        /* Convert to decimal string representation.
         * Worst case: sign(1) + "0."(2) + leading_zeros(scale, max 38)
         * + digits(39) + null(1) = 81 bytes. Use 128 for safety. */
        char buf[128];
        bool negative = (hi >> 63) != 0;

        /* Two's complement for negative */
        if (negative)
        {
            lo = ~lo;
            hi = ~hi;
            lo++;
            if (lo == 0)
                hi++;
        }

        /* Convert 128-bit unsigned to string using digit extraction */
        char digits[40];
        int ndigits = 0;

        if (hi == 0 && lo == 0)
        {
            digits[ndigits++] = '0';
        }
        else
        {
            /* Process digits by repeated division by 10 */
            uint64_t h = hi, l = lo;
            while (h > 0 || l > 0)
            {
                /* Divide 128-bit number by 10 */
                uint64_t remainder = 0;
                uint64_t new_h, new_l;

                /* Divide high part */
                new_h = h / 10;
                remainder = h % 10;

                /* Divide low part with carry from high */
                uint64_t temp = (remainder << 32) | (l >> 32);
                uint64_t q_hi = temp / 10;
                remainder = temp % 10;
                temp = (remainder << 32) | (l & 0xFFFFFFFF);
                uint64_t q_lo = temp / 10;
                remainder = temp % 10;

                new_l = (q_hi << 32) | q_lo;

                digits[ndigits++] = '0' + (char) remainder;
                h = new_h;
                l = new_l;
            }
        }

        /* Build the string with decimal point */
        int pos = 0;
        if (negative)
            buf[pos++] = '-';

        if (scale <= 0 || ndigits <= scale)
        {
            /* Need leading zeros: 0.00...digits */
            buf[pos++] = '0';
            if (scale > 0)
            {
                buf[pos++] = '.';
                int zeros = scale - ndigits;
                int z;
                for (z = 0; z < zeros; z++)
                    buf[pos++] = '0';
                int d;
                for (d = ndigits - 1; d >= 0; d--)
                    buf[pos++] = digits[d];
            }
        }
        else
        {
            /* Integer part + decimal part */
            int int_digits = ndigits - scale;
            int d;
            for (d = ndigits - 1; d >= scale; d--)
                buf[pos++] = digits[d];
            if (scale > 0)
            {
                buf[pos++] = '.';
                for (d = scale - 1; d >= 0; d--)
                    buf[pos++] = digits[d];
            }
        }
        buf[pos] = '\0';

        /* Use numeric_in to parse the string */
        Datum result = DirectFunctionCall3(numeric_in,
                                           CStringGetDatum(buf),
                                           ObjectIdGetDatum(InvalidOid),
                                           Int32GetDatum(-1));
        return result;
    }

    /*
     * List types: "+l" (list), "+L" (large_list), "+w:N" (fixed_size_list)
     *
     * For the MVP, we convert list elements to a PG array.
     * The list has one child (the element array).
     */
    if ((fmt[0] == '+' && fmt[1] == 'l' && fmt[2] == '\0') ||
        (fmt[0] == '+' && fmt[1] == 'L' && fmt[2] == '\0'))
    {
        if (array->n_children < 1 || array->children == NULL ||
            array->children[0] == NULL || schema->children == NULL ||
            schema->children[0] == NULL)
        {
            *isnull = true;
            return (Datum) 0;
        }
        struct ArrowArray *child = array->children[0];
        struct ArrowSchema *child_schema = schema->children[0];
        int64_t idx = row + array->offset;
        int64_t start, end;

        if (fmt[1] == 'l')
        {
            const int32_t *offsets = (const int32_t *) array->buffers[1];
            start = offsets[idx];
            end = offsets[idx + 1];
        }
        else
        {
            const int64_t *offsets = (const int64_t *) array->buffers[1];
            start = offsets[idx];
            end = offsets[idx + 1];
        }

        int64_t list_len_64 = end - start;
        if (list_len_64 < 0 || list_len_64 > 1000000)
        {
            elog(ERROR, "lance_fdw: list length %ld exceeds limit (max 1000000)",
                 (long)list_len_64);
        }
        int32_t list_len = (int32_t)list_len_64;

        /* Determine element type info */
        Oid elem_type = get_element_type(pg_type_oid);
        if (elem_type == InvalidOid)
            elem_type = pg_type_oid;    /* fallback */

        int16 elem_typlen;
        bool  elem_typbyval;
        char  elem_typalign;
        get_typlenbyvalalign(elem_type, &elem_typlen, &elem_typbyval, &elem_typalign);

        Datum *elems = (Datum *) palloc(sizeof(Datum) * list_len);
        bool  *nulls = (bool *) palloc(sizeof(bool) * list_len);
        int i;

        for (i = 0; i < list_len; i++)
        {
            elems[i] = arrow_value_to_datum(child, child_schema,
                                            start + i, elem_type, &nulls[i]);
        }

        int lbound = 1;
        ArrayType *arr = construct_md_array(elems, nulls,
                                            1, &list_len, &lbound,
                                            elem_type, elem_typlen,
                                            elem_typbyval, elem_typalign);
        pfree(elems);
        pfree(nulls);
        return PointerGetDatum(arr);
    }

    /* Fixed-size list "+w:N" */
    if (fmt[0] == '+' && fmt[1] == 'w' && fmt[2] == ':')
    {
        if (array->n_children < 1 || array->children == NULL ||
            array->children[0] == NULL || schema->children == NULL ||
            schema->children[0] == NULL)
        {
            *isnull = true;
            return (Datum) 0;
        }
        int fixed_size = atoi(fmt + 3);
        if (fixed_size <= 0 || fixed_size > 1000000)
        {
            elog(ERROR, "lance_fdw: fixed list size %d exceeds limit", fixed_size);
        }
        struct ArrowArray *child = array->children[0];
        struct ArrowSchema *child_schema = schema->children[0];
        int64_t idx = row + array->offset;
        int64_t start = idx * fixed_size;

        Oid elem_type = get_element_type(pg_type_oid);
        if (elem_type == InvalidOid)
            elem_type = pg_type_oid;

        int16 elem_typlen;
        bool  elem_typbyval;
        char  elem_typalign;
        get_typlenbyvalalign(elem_type, &elem_typlen, &elem_typbyval, &elem_typalign);

        Datum *elems = (Datum *) palloc(sizeof(Datum) * fixed_size);
        bool  *nulls = (bool *) palloc(sizeof(bool) * fixed_size);
        int i;

        for (i = 0; i < fixed_size; i++)
        {
            elems[i] = arrow_value_to_datum(child, child_schema,
                                            start + i, elem_type, &nulls[i]);
        }

        int lbound2 = 1;
        ArrayType *arr = construct_md_array(elems, nulls,
                                            1, &fixed_size, &lbound2,
                                            elem_type, elem_typlen,
                                            elem_typbyval, elem_typalign);
        pfree(elems);
        pfree(nulls);
        return PointerGetDatum(arr);
    }

    /*
     * Struct "+s" - serialize fields to JSON-like text.
     * Full composite type support (heap_form_tuple) requires TupleDesc lookup
     * which depends on the PG type being a pre-created composite type.
     * For now, serialize as {"field1": val1, "field2": val2, ...} text.
     */
    if (fmt[0] == '+' && fmt[1] == 's' && fmt[2] == '\0')
    {
        if (array->children == NULL || schema->children == NULL || array->n_children == 0)
        {
            *isnull = true;
            return (Datum) 0;
        }

        StringInfoData sbuf;
        initStringInfo(&sbuf);
        appendStringInfoChar(&sbuf, '{');

        int64_t ofs = row + array->offset;
        for (int64_t c = 0; c < array->n_children; c++)
        {
            if (c > 0)
                appendStringInfoString(&sbuf, ", ");

            const char *fname = (schema->children[c] && schema->children[c]->name)
                                ? schema->children[c]->name : "?";
            appendStringInfo(&sbuf, "\"%s\": ", fname);

            if (array->children[c] == NULL || arrow_is_null(array->children[c], ofs))
            {
                appendStringInfoString(&sbuf, "null");
            }
            else
            {
                /* Convert child value to its text representation */
                const char *child_fmt = schema->children[c]->format;
                bool is_string = child_fmt && (child_fmt[0] == 'u' || child_fmt[0] == 'U');

                if (is_string)
                {
                    /* String types: get the text datum directly */
                    bool child_null = false;
                    Datum child_val = arrow_value_to_datum(
                        array->children[c], schema->children[c],
                        ofs, TEXTOID, &child_null);
                    if (child_null)
                        appendStringInfoString(&sbuf, "null");
                    else
                    {
                        char *txt = TextDatumGetCString(child_val);
                        appendStringInfo(&sbuf, "\"%s\"", txt);
                        pfree(txt);
                    }
                }
                else
                {
                    /* Numeric/other types: use output function to get text */
                    bool child_null = false;
                    Datum child_val = arrow_value_to_datum(
                        array->children[c], schema->children[c],
                        ofs, FLOAT8OID, &child_null);
                    if (child_null)
                        appendStringInfoString(&sbuf, "null");
                    else
                    {
                        /* For simple scalar types, format via snprintf */
                        if (child_fmt && child_fmt[0] == 'f')
                            appendStringInfo(&sbuf, "%g", DatumGetFloat4(child_val));
                        else if (child_fmt && child_fmt[0] == 'g')
                            appendStringInfo(&sbuf, "%g", DatumGetFloat8(child_val));
                        else if (child_fmt && (child_fmt[0] == 'i'))
                            appendStringInfo(&sbuf, "%d", DatumGetInt32(child_val));
                        else if (child_fmt && (child_fmt[0] == 'l'))
                            appendStringInfo(&sbuf, "%ld", (long)DatumGetInt64(child_val));
                        else if (child_fmt && child_fmt[0] == 'b')
                            appendStringInfoString(&sbuf, DatumGetBool(child_val) ? "true" : "false");
                        else
                            appendStringInfoString(&sbuf, "\"?\"");
                    }
                }
            }
        }

        appendStringInfoChar(&sbuf, '}');
        return PointerGetDatum(cstring_to_text_with_len(sbuf.data, sbuf.len));
    }

    /*
     * Fallback: unsupported type
     */
    elog(WARNING, "lance_fdw: unsupported Arrow format '%s', returning NULL", fmt);
    *isnull = true;
    return (Datum) 0;
}

/*
 * Release an Arrow array exported via the C Data Interface.
 * Must be called when the batch is no longer needed.
 */
void
arrow_release_array(struct ArrowArray *array)
{
    if (array && array->release)
    {
        array->release(array);
        array->release = NULL;
    }
}

/*
 * Release an Arrow schema exported via the C Data Interface.
 */
void
arrow_release_schema(struct ArrowSchema *schema)
{
    if (schema && schema->release)
    {
        schema->release(schema);
        schema->release = NULL;
    }
}
