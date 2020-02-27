"""Model for the ISIS master file format.

Part of the ISIS-mode packed unshifted file format specification
can be found at:

https://unesdoc.unesco.org/ark:/48223/pf0000211280

The unpacked, FFI, shifted and other customized file formats
were discovered based on the analysis of actual MST files
as well as the source code of CISIS and Bruma.
"""
from binascii import b2a_hex
from itertools import accumulate

from construct import Array, BitsInteger, BitStruct, \
                      Byte, Bytes, ByteSwapped, \
                      Check, CheckError, Computed, Const, \
                      Default, ExprAdapter, Flag, FocusedSeq, \
                      Int16sb, Int16sl, Int16ub, Int16ul, \
                      Int32sb, Int32sl, Int32ub, Int32ul, \
                      Padded, Padding, Rebuild, Select, SelectError, Struct, \
                      Tell, Terminated, Union

from .ccons import DictSegSeq, Unnest


DEFAULT_MST_ENCODING = "cp1252"
DEFAULT_ENDIANNESS = "little"
DEFAULT_FORMAT = "isis"
DEFAULT_LOCKABLE = True
DEFAULT_SHIFT = 6
DEFAULT_SHIFT4IS3 = False
DEFAULT_MIN_MODULUS = 2
DEFAULT_PACKED = False
DEFAULT_FILLER = b"\x00"
DEFAULT_RECORD_FILLER = b" "
DEFAULT_CONTROL_LEN = 64
DEFAULT_IBP = "check"


def con_pairs(con):
    """Generator of raw ``(tag, field)`` pairs of ``bytes`` objects."""
    for dir_entry, field_value in zip(con.dir, con.fields):
        yield b"%d" % dir_entry.tag, field_value


def tl2con(tl):
    """Create a record dict that can be used for MST building
    from a single tidy list record."""
    container = {
        "dir": [],
        "fields": [],
    }
    for k, v in tl:
        if k == b"mfn":
            container["mfn"] = int(v)
        else:
            container["dir"].append({"tag": int(k)})
            container["fields"].append(v)
    return container


def pad_size(modulus, size):
    """Calculate the padding size for the given size in a modulus grid."""
    return (modulus * (size // modulus + 1) - size) % modulus


def never_split_pad_size(address, leader_len):
    """Calculate the padding until the end of block for the next record."""
    offset = address & 0x1ff
    return 0 if offset + leader_len - 4 <= 512 else 512 - offset


class StructCreator:
    """
    Creator of MST record structs.

    Parameters
    ----------
    endianness : str
        Byte order endianness for all 16/32 bits integer numbers,
        might be "big" or "little" (a.k.a. swapped).
    format : str
        Format mode, a string that should either be "isis" or "ffi".
        The FFI is a format mode that supports bigger records,
        with sizes up to 2_147_483_647 instead of 32767
        (or twice that plus one if lockable is ``False``).
        To do that,
        the records in the FFI format mode have 4 bytes instead of 2
        in two leader fields (``mfrl`` and ``base_addr``)
        and two directory fields (``pos`` and ``len``).
    lockable : bool
        Multi-user locking, enabling three locking mechanisms:
        ``delock_count`` (Data entry lock),
        ``ewlock`` (Exclusive write lock), and
        ``rlock`` (Record lock).
        The latter is stored in the ``mfrl`` sign of a record,
        the former ones are simply an interpretation
        of respectively the ``mfcxx2`` and ``mfcxx3`` fields
        from the control record,
        where the ``delock_count`` is a counter
        of how many records has the ``rlock``,
        and ``ewlock`` is a file-level lock
        for a single application to block other ones from writing,
        e.g. during a backup/restore operation.
        Locked records might have a 9999 tag,
        which would include the timestamp and user who locked it.
    default_shift : int
        Default XRF shifting for building,
        it tells how many bits should be shifted in the XRF pointers.
        On parsing, this is obtained from the ``mstxl`` field
        (the most significant byte of the legacy MSTYPE field)
        in the control record.
    shift4is3 : bool
        Flag for the legacy shifting interpretation
        where a shift of 4 should be regarded as 3.
    min_modulus : int
        Smallest padding "modulus" for MST records alignment.
        The modulus is ``2 ** shift``,
        unless this number is higher than that.
        It must be a power of 2.
        The standard ISIS defines it as 2.
    packed : bool
        Flag for packing the leader and directory fields
        with no filler/padding/slack bytes for DWORD alignment.
        It disables the 4-bytes alignment
        of all 4-bytes integer fields.
        If ``False``, it adds a WORD-sized (16 bits) padding
        for DWORD-sized (32 bits) alignment
        in all fields that requires it, that is,
        this padding filler appears after the ``mfrl`` in ISIS,
        and after ``base_addr`` and ``tag`` in FFI.
        The origin of these fillers for data structure alignment
        is historical, coming from the usage of raw structs in C
        without neither ``#pragma pack(2)`` or ``-fpack-struct=2``
        or anything else that enforces the same packing,
        and the fact that CISIS stores data in master files
        in the same way they are stored in memory,
        reflecting any implementation detail from the ABI,
        the C/C++ compiler and the architecture.
        Most C/C++ compilers, including GCC,
        standardized the data alignment to always have
        ``int``/``long`` (4 bytes fields) 4-byte aligned
        in 32 bits architectures,
        so this flag should almost always be ``False``
        for files created with CISIS,
        although it's not part of the original ISIS format.
        The file format has nothing specific to the operating system,
        but ``packed=False'' might also be called "Linux version",
        and ``packed=True'' the "Windows version"
        in some Bruma and CISIS code and documentation,
        because CISIS used to be compiled with ``-fpack-struct=1``
        in the latter as a consequence of a custom IDE configuration,
        something that had never been applied
        when compiling it on Linux.
        Bruma names this using the number of slack bytes,
        where its *align2* means ``packed=False''
        and its *align0* means ``packed=True'',
        although the actual alignment for each case
        is respectively 4-bytes and 2-bytes.
    filler : bytes,
        Default filler character,
        used to replace ``None`` in the remaining filler arguments.
    control_filler : bytes or None
        Filler character for the trailing bytes of the control record.
        In some obscure cases of this might be ``"\\xff"``,
        as CISIS describes for Unisys.
    slack_filler : bytes or None
        Padding filler character
        to appear between some leader and directory fields
        when ``packed=False''.
    block_filler : bytes or None
        Filler character for trailing recordless bytes of a block.
    record_filler : bytes or None
        Filler character for the trailing record data
        that doesn't belong to any field,
        but appears due to the record alignment.
    control_len : int
        Control record length,
        it should be at least ``max(32, 2 ** shift)``.
    ibp : str
        Invalid block padding content/size acceptance mode.
        There are 3 modes:
        "check", to raise an exception when such invalid data appears;
        "ignore", to skip these bytes;
        and "store", to add an "ibp" field
        with the found trailing invalid data in hex.
    """
    def __init__(
        self,
        endianness=DEFAULT_ENDIANNESS,
        format=DEFAULT_FORMAT,
        lockable=DEFAULT_LOCKABLE,
        default_shift=DEFAULT_SHIFT,
        shift4is3=DEFAULT_SHIFT4IS3,
        min_modulus=DEFAULT_MIN_MODULUS,
        packed=DEFAULT_PACKED,
        filler=DEFAULT_FILLER,
        control_filler=None,
        slack_filler=None,
        block_filler=None,
        record_filler=DEFAULT_RECORD_FILLER,
        control_len=DEFAULT_CONTROL_LEN,
        ibp=DEFAULT_IBP,
    ):
        # Get the actual value for every filler
        self.control_filler = \
            filler if control_filler is None else control_filler
        self.slack_filler = \
            filler if slack_filler is None else slack_filler
        self.block_filler = \
            filler if block_filler is None else block_filler
        self.record_filler = \
            filler if record_filler is None else record_filler

        # Validation for inputs that wouldn't break somewhere else
        if endianness not in ["big", "little"]:
            raise ValueError("Invalid endianness")
        if format not in ["isis", "ffi"]:
            raise ValueError("Invalid format mode")
        if ibp not in ["check", "ignore", "store"]:
            raise ValueError("Invalid ibp mode")
        min_modulus_rounded_to_powerof2 = 1 << (min_modulus.bit_length() - 1)
        if min_modulus <= 0 or min_modulus != min_modulus_rounded_to_powerof2:
            raise ValueError("Value of min_modulus must be "
                             "a positive power of 2")
        if control_len % min_modulus != 0:
            raise ValueError("Control record length "
                             "isn't a multiple of min_modulus")
        if any(len(f) != 1 for f in [filler,
                                     self.control_filler, self.slack_filler,
                                     self.block_filler, self.record_filler]):
            raise ValueError("All filler patterns lengths must be 1")

        # Store the inputs
        self.endianness = endianness
        self.format = format
        self.lockable = lockable
        self.default_shift = default_shift
        self.shift4is3 = shift4is3
        self.min_modulus = min_modulus
        self.packed = packed
        self.control_len = control_len
        self.ibp = ibp

    def create_control_record_struct(self):
        little_endian = self.endianness == "little"
        min_mod = self.min_modulus
        control_len = self.control_len

        # Data types for integer values
        Int16s, Int16u, Int32s, Int32u = {
            "big":    (Int16sb, Int16ub, Int32sb, Int32ub),
            "little": (Int16sl, Int16ul, Int32sl, Int32ul),
        }[self.endianness]

        # The 16 bytes that used to have the legacy MFTYPE field
        # has both the actual MFTYPE and the MSTXL,
        # and their order is affected by the endianness
        mftype = "mftype" / Default(Byte, 0)
        mstxl = "mstxl" / Rebuild(
            Byte,
            lambda this:
                this.mstxl if "mstxl" in this and this.mstxl is not None else
                this.get(
                    "shift",
                    this.modulus.bit_length() - 1
                        if "modulus" in this else self.default_shift,
                ),
        )

        # MFCXX2 and MFCXX3 are used for locking the file on updating
        if self.lockable:
            mfcxx2 = "_mfcxx2" / Union(0,
                "mfcxx2" / Default(Int32s, 0),
                "delock_count" / Int32u,  # Data entry lock
            )
            mfcxx3 = "_mfcxx3" / Union(0,
                "mfcxx3" / Default(Int32s, 0),
                "ewlock" / ExprAdapter(Int32u,  # Exclusive write lock
                                       lambda obj, ctx: bool(obj),
                                       lambda obj, ctx: int(obj)),
            )
        else:
            mfcxx2 = "mfcxx2" / Default(Int32s, 0)
            mfcxx3 = "mfcxx3" / Default(Int32s, 0)

        # Legacy shift replacement
        if self.shift4is3:
            shift = "shift" / Computed(lambda this:
                                       3 if this.mstxl == 4 else this.mstxl)
        else:
            shift = "shift" / Computed(lambda this: this.mstxl)

        # The control record struct for all cases
        nested_unpadded_struct = Struct(
            # First 4 fields, including information about the whole file
            "mfn" / Default(Int32s, 0),  # CTLMFN
            Check(lambda this: this.mfn == 0),
            "next_mfn" / Default(Int32s, 1),  # NXTMFN
            "next_block" / Default(Int32s, 1),  # NXTMFB
            "next_offset" / Default(Int16u, self.control_len),  # NXTMFP
            *([mftype, mstxl] if little_endian else [mstxl, mftype]),

            # Get the MST (modulus) and XRF (shift) alignment/shift values
            # from the MSTXL field, fixing the "legacy" replacements
            shift,
            "modulus" / Computed(lambda this: max(min_mod, 1 << this.shift)),
            Check(lambda this: control_len % this.modulus == 0),

            # Fields used for "statistics during backup/restore",
            # where the last two fields are also used for multi-user locking
            "reccnt" / Default(Int32s, 0),
            "mfcxx1" / Default(Int32s, 0),
            mfcxx2,
            mfcxx3,
        )
        return Padded(
            length=control_len,
            subcon=Unnest(["_mfcxx2", "_mfcxx3"], nested_unpadded_struct)
                if self.lockable else nested_unpadded_struct,
            pattern=self.control_filler,
        )

    def create_record_struct(self, control_record):
        # Pre-computed lengths and flags
        ffi = self.format == "ffi"
        slacked = not self.packed
        leader_len = 18 + 4 * ffi + 2 * slacked
        dir_entry_len = 6 + 4 * ffi + 2 * (ffi & slacked)
        min_mod = max(self.min_modulus, 1 << self.default_shift)
        inf = float("inf")

        # Data types for integer values (strings are always big endian)
        Int16s, Int16u, Int32s, Int32u = {
            "big":    (Int16sb, Int16ub, Int32sb, Int32ub),
            "little": (Int16sl, Int16ul, Int32sl, Int32ul),
        }[self.endianness]

        # For the leader and directory structure of the records,
        # some sizes are configured based on the ffi and lockable flags
        # (MRFL is the record length)
        if self.lockable:
            mfrl_fields = [
                "mfrl" / Rebuild(
                    Int32s if ffi else Int16s,
                    lambda this:
                        -this._build_total_len
                        if this.get("rlock", False) else
                        this._build_total_len
                ),
                "total_len" / Computed(lambda this: abs(this.mfrl)),
                "rlock" / Computed(lambda this: this.mfrl < 0),
            ]
        else:
            mfrl_fields = [
                "mfrl" / Rebuild(Int32u if ffi else Int16u,
                                 lambda this: this._build_total_len),
                "total_len" / Computed(lambda this: this.mfrl),
            ]

        return Struct(
            # Block alignment ("never splits" the leader unless BASE fits)
            "_before_start" / Tell,
            Padding(
                lambda this:
                    never_split_pad_size(this._before_start, leader_len),
                self.block_filler,
            ),

            "_start" / Tell,

            # Build time pre-computed information
            "_build_len_list" / Computed(
                lambda this: None if "fields" not in this else
                    [len(field) for field in this.fields]
            ),
            "_build_pos_list" / Computed(
                lambda this: None if "fields" not in this else
                    list(accumulate([0] + this._build_len_list))
            ),
            "_build_total_len_padless" / Computed(
                lambda this: None if "fields" not in this else
                    leader_len +
                    dir_entry_len * len(this.fields) +
                    this._build_pos_list[-1]  # Fields length
            ),
            "_build_total_len" / Computed(
                lambda this: None if "fields" not in this else
                    this._build_total_len_padless +
                    pad_size(control_record.get("modulus", min_mod),
                             this._build_total_len_padless)
            ),

            # Record leader/header
            "mfn" / Int32s,  # Master file number
            Check(lambda this: this.mfn != 0),  # Not a control record
            Check(lambda this: control_record.get("next_mfn", inf) > this.mfn),
            *mfrl_fields,

            Check(lambda this:
                  this.total_len % control_record.get("modulus", min_mod)
                  == 0),
            *([Const(self.slack_filler * 2)] if slacked and not ffi else []),
            "old_block" / Default(Int32s, 0),  # MFBWB backward pointer block
            "old_offset" / Default(Int16u, 0),  # MFBWP backward pointer offset
            *([Const(self.slack_filler * 2)] if slacked and ffi else []),
            "base_addr" / Rebuild(
                Int32u if ffi else Int16u,
                lambda this: leader_len + dir_entry_len * len(this.fields),
            ),
            "num_fields" / Rebuild(Int16u,  # NVF
                                   lambda this: len(this.fields)),
            "status" / Default(Int16u, 0),  # Active is 0, deleted is 1

            # Directory
            "dir" / Struct(
                "tag" / Int16u,
                *([Const(self.slack_filler * 2)] if slacked and ffi else []),
                "pos" / Rebuild(
                    Int32u if ffi else Int16u,
                    lambda this: this._._build_pos_list[this._index],
                ),
                "len" / Rebuild(
                    Int32u if ffi else Int16u,
                    lambda this: this._._build_len_list[this._index],
                ),
            )[lambda this: this.num_fields],
            "_before_fields" / Tell,
            Check(lambda this:
                  this.base_addr == this._before_fields - this._start),

            # Field data
            "fields" / Array(
                lambda this: this.num_fields,
                "value" / Bytes(lambda this: this.dir[this._index].len),
            ),

            # Record alignment (implicitly checks the length stored in MFRL)
            "_after_fields" / Tell,
            Padding(
                lambda this:
                    this.total_len - (this._after_fields - this._start),
                self.record_filler,
            ),
        )

    def create_ending_struct(self):
        return FocusedSeq(
            "empty",
            "_before_start" / Tell,
            Padding(
                lambda this: 512 - (this._before_start & 0x1ff),
                self.block_filler,
            ),
            "empty" / Terminated,
        )

    def create_xrf_struct(self, control_record):
        shift = control_record.shift
        Int32s, BitStructWrapper = {
            "big": (Int32sb, lambda x: x),
            "little": (Int32sl, ByteSwapped),
        }[self.endianness]
        return FocusedSeq(
            "data",
            "data" / DictSegSeq(
                idx_field=Int32s,
                subcon=BitStructWrapper(BitStruct(
                    "block" / Default(BitsInteger(21 + shift, signed=True), 0),
                    "is_new" / Default(Flag, False),
                    "is_updated" / Default(Flag, False),
                    "offset" / Default(ExprAdapter(
                        BitsInteger(9 - shift),
                        lambda obj, context: obj << shift,
                        lambda obj, context: obj >> shift,
                    ), 0),
                )),
                block_size=127,  # Not counting the index field
                empty_item={},
                check_nonempty=lambda item: any([
                    item.block, item.offset, item.is_new, item.is_updated,
                ]),
            ),
            Terminated,
        )

    def iter_con(self, mst_stream, yield_control_record=False): # noqa: C
        """Generator of records as parsed construct.Container objects.

        Parameters
        ----------
        mst_stream : file-like
            A seekable open stream of bytes in its initial position,
            where the MST binary contents should be read from.
        yield_control_record : bool
            Control if the first generated construct.Container object
            should be the control record,
            or if it should skip it to generate just the records.
        """
        ending_struct = self.create_ending_struct()
        control_record_struct = self.create_control_record_struct()
        control_record = control_record_struct.parse_stream(mst_stream)
        record_struct = self.create_record_struct(control_record)
        leader_len = 18 + 4 * (self.format == "ffi") + 2 * (not self.packed)
        rec_or_end_struct = Select(record_struct, ending_struct)

        last_tell = 0
        def record_ibp_gen():
            nonlocal last_tell
            ibps = []
            while True:
                try:
                    record = rec_or_end_struct.parse_stream(mst_stream)
                except SelectError:
                    if self.ibp == "check":
                        raise
                    elif self.ibp == "store":
                        ibps.append(mst_stream.read(control_record.modulus))
                    continue
                if ibps:
                    yield {"ibp": b2a_hex(b"".join(ibps))}
                    ibps.clear()
                if record is None:  # No more records
                    break
                last_tell = mst_stream.tell()
                yield record

        if yield_control_record:
            prev = control_record
        else:
            prev = None
        for record in record_ibp_gen():
            if record and "ibp" in record:
                prev.update(record)
                yield prev
                prev = None
            else:
                if prev:
                    yield prev
                prev = record
        if prev:
            yield prev

        next_addr = last_tell + never_split_pad_size(last_tell, leader_len)
        next_block = 1 + (next_addr >> 9)
        next_offset = 1 + (next_addr & 0x1ff)
        if control_record.next_block != next_block:
            raise CheckError("Invalid next_block")
        if control_record.next_offset != next_offset:
            raise CheckError("Invalid next_offset")

    def iter_raw_tl(self, mst_stream, *,
                    only_active=True, prepend_mfn=False, prepend_status=False,
    ):
        for con in self.iter_con(mst_stream):
            if con.get("old_block", 0) != 0 or con.get("old_offset", 0) != 0:
                raise NotImplementedError("Pending master file reorganization")
            if only_active and con.status != 0:
                continue
            result = []
            if prepend_mfn:
                result.append((b"mfn", b"%d" % con.mfn))
            if prepend_status:
                result.append((b"status", b"%d" % con.status))
            result.extend(con_pairs(con))
            if "ibp" in con and self.ibp == "store":
                result.append((b"ibp", con["ibp"]))
            yield result

    def build_stream(self, records, mst_stream, control_record=None):
        """Build the MST binary data on the given seekable stream.

        Parameters
        ----------
        records : iterable of dict or construct.Container instances
            A preferably lazy iterable (generator)
            with the records to be stored, but the control_record,
            ordered by their MFN.
            If the records don't include the MFN number,
            a serial one will be included,
            resuming it from the biggest MFN previously included.
        mst_stream : file-like
            An empty, writeable and seekable open stream of bytes
            to store the MST binary contents.
        control_record : dict or construct.Container
            A custom control record object,
            allowing the storage of data in fields like reccnt.
        """
        ending_struct = self.create_ending_struct()

        control_record_struct = self.create_control_record_struct()
        if control_record is None:
            control_record = {}
        control_record_struct.build_stream(control_record, mst_stream)
        mst_stream.flush()

        record_struct = self.create_record_struct(control_record)
        leader_len = 18 + 4 * (self.format == "ffi") + 2 * (not self.packed)

        next_mfn = 1
        for record in records:
            if "mfn" in record:
                next_mfn = max(next_mfn, record["mfn"] + 1)
            else:
                record["mfn"] = next_mfn
                next_mfn += 1
            record_struct.build_stream(record, mst_stream)
            mst_stream.flush()
        last_tell = mst_stream.tell()
        ending_struct.build_stream(None, mst_stream)
        mst_stream.flush()

        next_addr = last_tell + never_split_pad_size(last_tell, leader_len)
        control_record["next_mfn"] = next_mfn
        control_record["next_block"] = 1 + (next_addr >> 9)
        control_record["next_offset"] = 1 + (next_addr & 0x1ff)

        end_tell = mst_stream.tell()
        mst_stream.seek(0)
        control_record_struct.build_stream(control_record, mst_stream)
        mst_stream.flush()
        mst_stream.seek(end_tell)
