(function () {
    let forks = [];

    for (var i = 0; i < $.forkCount; ++i) {
        forks.push({});
    }

    return forks;
})();