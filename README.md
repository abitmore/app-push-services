# app-push-services

BitShares Mobile App push service, using third-party software such as telegram.

## Static Linking

#### Step 1. Clone repository.
```
git clone https://github.com/bitshares/app-push-services.git
```

#### Step 2. Run compile docker and install dependencies tools.
```
docker run -it --rm  -v $(pwd):/workspace -w /workspace crystallang/crystal:1.3.2-alpine
/workspace # apk add autoconf cmake make automake libtool g++ # Install dependencies tools
```

#### Step 3. Build.
```
shards build --release --static
```

## Run
```
./app_pusher --token=YOUR_TELEGRAM_BOT_TOEKN
```

## Contributing

1. Fork it (<https://github.com/bitshares/app-push-services/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [syalon](https://github.com/syalon) - creator and maintainer
