Yii2 Fluentd
============

Yii2 Fluentd adds a Log Target for Fluentd.

The bundled client implementation is a relatively fast fire-and-forget client
for a Fluentd HTTP input plugin. However, you may extend the
`FluentClientInterface` to implement whatever client implementation
fits your needs.

For license information check the [LICENSE](LICENSE.md)-file.


Installation
------------

The preferred way to install this extension is through [composer](http://getcomposer.org/download/).

```
php composer.phar require --prefer-dist thamtech/yii2-fluentd
```

or add

```
"thamtech/yii2-fluentd": "*"
```

to the `require` section of your `composer.json` file.
