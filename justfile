git-pull-all-braches:
  git branch -r | grep -v '\->' | sed "s,\x1B\[[0-9;]*[a-zA-Z],,g" | while read remote; do git branch --track "${remote#origin/}" "$remote" || :; done
  git pull --all

make_test_zip:
  #!/usr/bin/env bash
  cd tests/SD/zip_contents
  zip ../SD$(date +%y%m%d%H%M).zip *

randomized_time_zip:
  bash scripts/01-storeFilter_and_queuing.sh

push:
  bash scripts/push_container.sh