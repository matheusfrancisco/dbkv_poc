use dbkv::DBKV;
use std::collections::HashMap;
//before this
//when a key is accessed with the get operation,
//to find its location on disk, we first need to load the
//index from disk and convert it to its in-memory form

// if you want you can map the target_arch  (aarch64, arm, mips, power-pc, x86_64) too
// also target_family (unix, windows)
// target_env (gnu, msvc, musl,  "")
// target_pointer_width (32, 64, 16) // The size (in bits) of the target architecture’s pointer. Used for isize, usize, * const, and * mut types.
// target_has_atomic (8, 16, 32, 64, 128, ptr) //Integer sizes that have support for atomic operations. During atomic operations, the CPU takes responsibility for preventing race conditions with shared data at the expense of performance. The word atomic is used in the sense of indivisible.
#[cfg(not(target_os = "windows"))]
const USAGE: &str = "
Usage:
    diskdbkv <command> [options]
Options:
    -h, --help      Show this help message
Commands:
    diskdbkv FILE get KEY     Get the value for KEY from the database in FILE
    diskdbkv FILE insert KEY VALUE   Set the value for KEY to VALUE in the database in FILE
    diskdbkv FILE delete KEY  Delete the entry for KEY from the database in FILE
    diskdbkv FILE update KEY VALUE  Update the value for KEY to VALUE in the database in FILE
";

type ByteStr = [u8];
type ByteString = Vec<u8>;

fn store_index_on_disk(a: &mut DBKV, index_key: &ByteStr) {
    a.index.remove(index_key);
    let index_as_bytes = bincode::serialize(&a.index).unwrap();
    a.index = std::collections::HashMap::new();
    a.insert(index_key, &index_as_bytes).unwrap();
}

fn main() {
    const INDEX_KEY: &ByteStr = b"+index";

    let args: Vec<String> = std::env::args().collect();
    let fname = args.get(1).expect(&USAGE);
    let action = args.get(2).expect(&USAGE).as_ref();
    let key = args.get(3).expect(&USAGE).as_ref();
    let maybe_value = args.get(4);

    let path = std::path::Path::new(&fname);

    let mut store = DBKV::open(path).expect("Failed to open database"); // opens the db file
    store.load().expect("Failed to load database"); // this will create a index in memory
    //
    //loads the offsets of any pre-existing data into an in-memory index.
    //The code uses two type aliases, ByteStr and ByteString:

    match action {
        "get" => {
            let index_as_bytes = store.get_disk(&INDEX_KEY).unwrap().unwrap();
            let index_decoded = bincode::deserialize(&index_as_bytes);
            let index: HashMap<ByteString, u64> = index_decoded.unwrap();

            match index.get(key) {
                None => eprintln!("{:?} not found", key),
                Some(&i) => {
                    let kv = store.get_at(i).unwrap();
                    println!("{:?}", kv.value)
                }
            }
        }
        "delete" => store.delete(key).unwrap(),
        "insert" => {
            let value = maybe_value.expect(&USAGE).as_ref();
            store.insert(key, value).unwrap();
            store_index_on_disk(&mut store, INDEX_KEY);
        }

        "update" => {
            let value = maybe_value.expect(&USAGE).as_ref();
            store.update(key, value).unwrap();
            store_index_on_disk(&mut store, INDEX_KEY);
        }
        _ => {
            eprintln!("{}", USAGE);
            return;
        }
    }
}
