use std::net::IpAddr;

pub fn addr_check(addr: &str) -> bool {
    let ip: Result<IpAddr, _> = addr
        .split(':')
        .next()
        .expect("correct ip:port format")
        .parse();
    let port: Result<u32, _> = addr
        .split(':')
        .last()
        .expect("should give a correct port info")
        .parse();
    !(ip.is_err() || port.is_err())
}
