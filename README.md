Projects
========

210
---

SR. Refactoring of Will Chen's original code to collate raw data files.

210_new
-------

Updated from `210`, to use the convention I adpoted for later projects of
keeping all original columns in their original order.

az
--

SR AstraZeneca screening project.

nb
--

SR "New Background" project. Technical replicates in horizontal pairs.

20161103_kidney_livetox
-----------------------

SR. Technical replicates in 2x2 squares but I left the horizontal pair logic in
place -- there are no missing data so the issue was moot.

20170117_beatriz_pfizer
-----------------------

BC. Sim1/3 use design on first sheet of platemap file, Sim2/4 use design on
second sheet. Technical replicates in horizontal pairs.

20170127_beatriz_pfizer
-----------------------

BC. Technical replicates in horizontal pairs.

New wrinkle: Each replicate has different timepoints, so expected timepoints are
defined in a dict-of-lists. I might keep this structure going forward even for
homogeneous designs.

20170202_beatriz_ccb
--------------------

BC. Due to using fixed cells, plate `Sim_000001` is 24h and `Sim_000002` is 72h,
rather than the same plate being scanned at different times. The code for this
script has been modified fairly heavily, so do not use this one as a basis for
other live-cell projects.
