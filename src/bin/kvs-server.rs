extern crate clap;
use clap::{App, Arg};
use kvs::{KvStore, KvsError, Result, KvsEngine};
use std::net::TcpListener;
use std::process::exit;
use std::io::prelude::*; // 这玩意到底是啥玩意
use std::env::current_dir;
extern crate env_logger;
use log::error;

use env_logger::{Builder, Target};

use kvs::thread_pool::*;

fn valid(address :&String) -> bool {
    //检查是否有：，以及ip是合理的，也就是有3个点，并且每个值小于等于255
    let mut colon_number = 0;
    let mut point_number = 0;
    for item in address.chars() {
        if item == ':' {
            colon_number += 1;
            if colon_number > 1 {
                return false;
            }
        } else if item == '.' {
            point_number += 1;
            if point_number > 4 {
                return false;
            }
        } else if !(item >= '0' && item <= '9'){
            return false;
        }
    }
    
    if colon_number != 1 || point_number != 3 {
        return false;
    }

    return true;
}

fn main() -> Result<()> {
    Builder::new().init();

        
    let matches = App::new("kvs server")
        .version(env!("CARGO_PKG_VERSION"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .arg(Arg::from_usage("-e, --engine = <kvs/sled> 'choose one engine, default is kvs'").required(false))
        .arg(Arg::from_usage("-a, --addr = <IP Address With Port> 'set the related address'").required(false))
        .get_matches();

    let mut engine_selection = String::from("kvs"); 
    if let Some(engine) = matches.value_of("engine") {
        if engine == "sled" {
            engine_selection = String::from("sled");
        } else if engine != "kvs" {
            println!("Engine is Error!");
            exit(1);
        }
    }
    
    let mut address_with_port = String::from("");

    if let Some(address) = matches.value_of("addr") {
        //这边要加一个判断address是否符合要求
        if !valid(&address.to_string()) {
            println!("Please Enter the Corrent Address with IP:Port!");
            exit(1);
        }
        address_with_port = address.to_string();
    } else {
        address_with_port = String::from("127.0.0.1:4000");
    }
    
    error!("version is {}, ip with port address is {}, engine is {}", env!("CARGO_PKG_VERSION"), address_with_port, engine_selection);

    let listener = TcpListener::bind(address_with_port).expect("Failed and bind with the sender");

    let pool =  SharedQueueThreadPool::new(16)?;

    for stream in listener.incoming() {
        pool.spawn(move || match stream {
            Ok(mut stream) => {
                let mut buffer = [0u8; 100]; // 这边我们设定传输的长度不会超过100
                //println!("connection!");
                match stream.read(&mut buffer) {
                    Ok(_) => {
                        let buffer = String::from_utf8(buffer.to_vec()).unwrap();
                        let buffer = String::from(buffer.trim_end_matches(char::from(0)));
                        let command_vec: Vec<&str> = buffer.split(" ").collect();

                        let mut store = KvStore::open(current_dir().unwrap()).unwrap();
                        // let mut sled_kv = SledKvsEngine::open(current_dir()?)?;
                        // let store:&mut dyn KvsEngine + 'static = kv_store;

                        match command_vec[0] {
                            "set" => {
                                if command_vec.len() != 3 {
                                    println!("error command {}", buffer);
                                } else {
                                    match store.set(command_vec[1].to_string(), command_vec[2].to_string()) {
                                        Ok(()) => {
                                            ()
                                        }
                                        Err(_) => {
                                            println!("Set Failed!");
                                        }
                                    }
                                }
                            }
                            "rm" => {
                                if command_vec.len() != 2 {
                                    println!("error command {}", buffer);
                                } else {
                                    match store.remove(command_vec[1].to_string()) {
                                        Ok(()) => {
                                            ()
                                        }
                                        Err(KvsError::KeyNotFound) => {
                                            stream.write("Key not found".as_bytes()).expect("failed to write");
                                            println!("Remove Error: Key not found");
                                        }
                                        Err(_) => {
                                            println!("Remove Error");
                                        }
                                    }
                                }
                                
                            }
                            "get" => {
                                if command_vec.len() != 2 {
                                    println!("error command {}", buffer);
                                } else {
                                    match store.get(command_vec[1].to_string()) {
                                        Ok(Some(value)) => {
                                            stream.write(value.as_bytes()).expect("failed to write");
                                        }
                                        Ok(None) => {
                                            stream.write("Key not found".as_bytes()).expect("failed to write");
                                            println!("Get Error: Key not found");
                                        }
                                        Err(_) => {
                                            println!("Get Error");
                                        }
                                    }
                                }
                            }
                            _ => {
                                println!("error command {}", buffer);
                            }
                        }
                        
                        
                    },
                    Err(e) => {
                        println!("Failed to receive data: {}", e);
                    }
                }
            }
            Err(e) => error!("Connection failed: {}", e),
        })
        
    }

    Ok(())
}