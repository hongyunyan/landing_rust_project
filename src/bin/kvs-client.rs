extern crate clap;
use clap::{App, Arg, SubCommand};
use std::process::exit;
use std::env::current_dir;
use kvs::{KvStore, KvsError, Result};
use std::path::PathBuf;
use std::net::TcpStream;
use std::io::prelude::*;


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
    let matches = App::new("kvs client")
        .version(env!("CARGO_PKG_VERSION"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string. Return an error if the value is not written successfully.")
                .arg(Arg::with_name("KEY").help("A string key").required(true))
                .arg(
                    Arg::with_name("VALUE")
                        .help("The string value of the key")
                        .required(true),
                )
                .arg(Arg::from_usage("-a, --addr = <IP Address With Port> 'set the related address'").required(false)),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a string key. If the key does not exist, return None. Return an error if the value is not read successfully.")
                .arg(Arg::with_name("KEY").help("A string key").required(true))
                .arg(Arg::from_usage("-a, --addr = <IP Address With Port> 'set the related address'").required(false)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key. Return an error if the key does not exist or is not removed successfully.")
                .arg(Arg::with_name("KEY").help("A string key").required(true))
                .arg(Arg::from_usage("-a, --addr = <IP Address With Port> 'set the related address'").required(false)),
        )
        .get_matches();

    match matches.subcommand() {
        ("set", Some(matches)) => {
            let key = matches.value_of("KEY").unwrap();
            let value = matches.value_of("VALUE").unwrap();
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

            //TODO:这边要加一个没有connect成功的处理。
            let mut stream = TcpStream::connect(address_with_port).unwrap();
            //println!("Connected to the server!");

            let input = String::from("set") + " " + key + " " + value; //统一用空格隔开

            stream.write(input.as_bytes()).expect("failed to write");
            //println!("Send input {}", input);

            Ok(())
        }
        ("get", Some(matches)) => {
            let key = matches.value_of("KEY").unwrap();
            
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

            let mut stream = TcpStream::connect(address_with_port).unwrap();
            //println!("Connected to the server!");

            let input = String::from("get") + " " + key;

            stream.write(input.as_bytes()).expect("failed to write");
            stream.flush()?;

            //println!("Send input {}", input);
            let mut buffer = String::new();
            match stream.read_to_string(&mut buffer) {
                Ok(_) => {
                    println!("{}", buffer);
                } 
                Err(e) => {
                    println!("Failed to receive data: {}", e);
                }
            }

            Ok(())
            
        }
        ("rm", Some(matches)) => {
            let key = matches.value_of("KEY").unwrap();

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

            let mut stream = TcpStream::connect(address_with_port).unwrap();
            //println!("Connected to the server!");

            let input = String::from("rm") + " " + key;

            stream.write(input.as_bytes()).expect("failed to write");
            stream.flush()?;

            let mut buffer = String::new();
            match stream.read_to_string(&mut buffer) {
                Ok(_) => {
                    if !buffer.is_empty() {
                        eprintln!("{}", buffer);
                        exit(1);
                    }
                    
                    
                } 
                Err(e) => {
                    println!("Failed to remove data: {}", e);
                    exit(1);
                }
            }


            Ok(())

            
        }
        _ => unreachable!(),
    }
}
