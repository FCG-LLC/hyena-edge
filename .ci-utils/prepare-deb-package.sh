#!/bin/bash
VERSION=$1

if [ $VERSION ]; then
    VERSION="-$VERSION"
fi

LAST_VERSION="$(head -n 1 debian/changelog)"
CHANGELOG_MSG="  * hyena development branch"
TRAILER_LINE=" -- DevOps <devops@collective-sense.com>  `date -R`"


echo -e "$LAST_VERSION\n\n$CHANGELOG_MSG\n\n$TRAILER_LINE\n\n$(cat debian/changelog)\n" > debian/changelog

sed -i "0,/)/ s/)/$VERSION)/" debian/changelog
