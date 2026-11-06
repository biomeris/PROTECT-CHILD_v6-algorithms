# Clocks available in the epiclock image

The 142 pyaging clocks built into the image (`clock_bundle`), as offered in the
web interface's clock list (same order). Generated from the bundle's metadata.

- **Predicts**: what the model estimates; not every pyaging "clock" estimates age.
- **Extra inputs**: inputs this algorithm does not provide (e.g. sex, age, protein
  levels). pyaging then uses reference values or 0, which can bias the result; the
  result reports these clocks under `clock_coverage` (`extra_features`).
- Defaults: `horvath2013`, `hannum`, `pcphenoage` and `pedbe` (paediatric buccal
  clock, for children's samples).

| Clock | Predicts | Tissue | Population | CpGs | Extra inputs | Year |
|---|---|---|---|---|---|---|
| `abec` | chronological age | whole blood | adults | 1695 |  | 2020 |
| `altumage` | chronological age | multi-tissue | all ages | 20318 |  | 2022 |
| `cabec` | chronological age | whole blood | adults | 1892 |  | 2020 |
| `corticalclock` | chronological age | brain cortex | all ages | 347 |  | 2020 |
| `ctsliver` | chronological age | liver | adults | 90 |  | 2024 |
| `eabec` | chronological age | whole blood | adults | 1791 |  | 2020 |
| `encen100` | chronological age | whole blood, saliva, buccal epithelium | centenarians | 198 |  | 2023 |
| `encen40` | chronological age | whole blood, saliva, buccal epithelium | older adults | 559 |  | 2023 |
| `gliasin` | chronological age | brain cortex | adults | 220 |  | 2024 |
| `han` | chronological age | whole blood | all ages | 65 |  | 2020 |
| `hannum` | chronological age | whole blood | adults | 71 |  | 2013 |
| `hep` | chronological age | liver | adults | 70 |  | 2024 |
| `horvath2013` | chronological age | multi-tissue | all ages | 353 |  | 2013 |
| `intrinclock` | chronological age | multi-tissue | all ages | 380 |  | 2024 |
| `intrinclock370` | chronological age | multi-tissue | all ages | 370 |  | 2024 |
| `lin` | chronological age | whole blood | all ages | 99 |  | 2016 |
| `neusin` | chronological age | brain cortex | adults | 672 |  | 2024 |
| `pcbrainage` | chronological age | brain cortex | adults | 357852 |  | 2022 |
| `pchannum` | chronological age | whole blood | adults | 78464 |  | 2022 |
| `pchorvath2013` | chronological age | multi-tissue | all ages | 78464 |  | 2022 |
| `pcskinandblood` | chronological age | skin, whole blood, cultured fibroblasts | all ages | 78464 |  | 2022 |
| `pedbe` | chronological age | buccal epithelium | children and adolescents | 94 |  | 2020 |
| `pipekelasticnet` | chronological age | multi-tissue | all ages | 239 |  | 2022 |
| `pipekfilteredh` | chronological age | multi-tissue | all ages | 272 |  | 2022 |
| `pipekretrainedh` | chronological age | multi-tissue | all ages | 308 |  | 2022 |
| `retroelementagev1` | chronological age | whole blood | all ages | 1317 |  | 2024 |
| `retroelementagev2` | chronological age | whole blood | all ages | 1378 |  | 2024 |
| `senchronoage` | chronological age | whole blood | adults | 187 |  | 2026 |
| `skinandblood` | chronological age | buccal epithelium, whole blood, epithelium, cultured fibroblasts, skin, cord blood | all ages | 391 |  | 2018 |
| `stoch` | chronological age | sorted monocytes | adults | 353 |  | 2024 |
| `stocp` | chronological age | sorted monocytes | adults | 513 |  | 2024 |
| `stocz` | chronological age | sorted monocytes | adults | 514 |  | 2024 |
| `vidalbralo` | chronological age | whole blood | adults | 8 |  | 2016 |
| `wu` | chronological age | whole blood | children | 111 |  | 2019 |
| `yingcausage` | chronological age | whole blood | adults | 585 |  | 2024 |
| `zhangblup` | chronological age | whole blood, saliva | all ages | 319607 |  | 2019 |
| `zhangen` | chronological age | whole blood, saliva | all ages | 514 |  | 2019 |
| `dnamphenoage` | phenotypic age | whole blood | adults | 513 |  | 2018 |
| `hrsinchphenoage` | phenotypic age | whole blood | adults | 959 |  | 2022 |
| `pcphenoage` | phenotypic age | whole blood | adults | 78464 |  | 2022 |
| `weidner` | biological age | whole blood | all ages | 3 |  | 2014 |
| `bohlin` | gestational age | cord blood | newborns | 251 |  | 2016 |
| `epicga` | gestational age | cord blood | newborns | 176 |  | 2021 |
| `knight` | gestational age | cord blood, neonatal blood spots | newborns | 148 |  | 2016 |
| `leecontrol` | gestational age | placenta | pregnancies | 546 |  | 2019 |
| `leerefinedrobust` | gestational age | placenta | pregnancies | 395 |  | 2019 |
| `leerobust` | gestational age | placenta | pregnancies | 558 |  | 2019 |
| `mayne` | gestational age | placenta | pregnancies | 62 |  | 2017 |
| `celldrift` | mitotic age | cultured human cells | human cell cultures | 2322 |  | 2023 |
| `epicmithyper` | mitotic age | B cells | human, age unspecified | 184 |  | 2020 |
| `epicmithypo` | mitotic age | B cells | human, age unspecified | 1164 |  | 2020 |
| `epitoc1` | mitotic age | multi-tissue, whole blood | all ages | 385 |  | 2016 |
| `epitoc2` | mitotic age | whole blood | adults | 163 |  | 2020 |
| `epitoc3` | mitotic age | cultured primary human cells, whole blood, multi-tissue, cord blood | all ages | 170 |  | 2020 |
| `hypoclock` | mitotic age | multi-tissue | human, age unspecified | 678 |  | 2020 |
| `miage` | mitotic age | multi-tissue | human, age unspecified | 268 |  | 2018 |
| `stemtoc` | mitotic age | multi-tissue, cultured human cells, whole blood | all ages | 371 |  | 2024 |
| `stemtocvitro` | mitotic age | multi-tissue, cultured human cells | prenatal and newborn | 629 |  | 2024 |
| `adbahadosingh` | late-onset Alzheimer's disease | whole blood | older adults | 4 |  | 2021 |
| `bocklandt` | EDARADD methylation | saliva | adults | 1 |  | 2011 |
| `cellpopage` | cell-population passage age | cultured fibroblasts | human cell cultures | 2543 |  | 2024 |
| `compil6` | interleukin-6 | whole blood | older adults | 35 |  | 2021 |
| `cvdwesterman` | cardiovascular disease risk | whole blood | adults | 235 |  | 2020 |
| `deconvolutebloodepicbcell` | B cell proportion | purified blood leukocytes | adults | 600 |  | 2018 |
| `deconvolutebloodepiccd4tcell` | CD4+ T cell proportion | purified blood leukocytes | adults | 600 |  | 2018 |
| `deconvolutebloodepiccd8tcell` | CD8+ T cell proportion | purified blood leukocytes | adults | 600 |  | 2018 |
| `deconvolutebloodepicmonocyte` | monocyte proportion | purified blood leukocytes | adults | 600 |  | 2018 |
| `deconvolutebloodepicneutrophil` | neutrophil proportion | purified blood leukocytes | adults | 600 |  | 2018 |
| `deconvolutebloodepicnkcell` | natural killer cell proportion | purified blood leukocytes | adults | 600 |  | 2018 |
| `depressionbarbu` | major depressive disorder | whole blood | adults | 196 |  | 2021 |
| `dnamfili` | frailty risk | whole blood | older adults | 20 |  | 2022 |
| `dnamic` | intrinsic capacity | whole blood | older adults | 91 |  | 2025 |
| `dnamphenoagenonprc` | non-polycomb PhenoAge contribution | whole blood | adults | 458 |  | 2022 |
| `dnamphenoageprc` | polycomb-related PhenoAge contribution | whole blood | adults | 55 |  | 2022 |
| `dnamstress` | stress exposure | whole blood | adults | 211 |  | 2023 |
| `dnamtl` | leukocyte telomere length | whole blood | adults | 140 |  | 2019 |
| `downsyndrome` | Down syndrome methylation score | neonatal blood spots | newborns | 652 |  | 2021 |
| `dunedinpace` | pace of aging | whole blood | adults | 20000 |  | 2022 |
| `dunedinpoam38` | pace of aging | whole blood | adults | 46 |  | 2020 |
| `garagnani` | ELOVL2 methylation | whole blood | all ages | 1 |  | 2012 |
| `grimage2leptin` | leptin | whole blood | older adults | 187 |  | 2022 |
| `grimage2logcrp` | C-reactive protein | whole blood | older adults | 132 |  | 2022 |
| `grimage2pai1` | PAI-1 | whole blood | older adults | 211 |  | 2022 |
| `hepatoxu` | hepatocellular carcinoma | plasma cell-free DNA | adults | 10 |  | 2017 |
| `mccartneyalcohol` | alcohol consumption | whole blood | adults | 450 |  | 2018 |
| `mccartneybmi` | body mass index | whole blood | adults | 1109 |  | 2018 |
| `mccartneybodyfat` | body fat percentage | whole blood | adults | 968 |  | 2018 |
| `mccartneyeducation` | educational attainment | whole blood | adults | 373 |  | 2018 |
| `mccartneyhdlcholesterol` | HDL cholesterol | whole blood | adults | 737 |  | 2018 |
| `mccartneyldlcholesterol` | LDL cholesterol | whole blood | adults | 233 |  | 2018 |
| `mccartneysmoking` | smoking exposure | whole blood | adults | 233 |  | 2018 |
| `mccartneytotalcholesterol` | total cholesterol | whole blood | adults | 204 |  | 2018 |
| `mccartneytotalhdlratio` | total-to-HDL cholesterol ratio | whole blood | adults | 412 |  | 2018 |
| `mccartneywhr` | waist-to-hip ratio | whole blood | adults | 226 |  | 2018 |
| `pcdnamtl` | leukocyte telomere length | whole blood | adults | 78464 |  | 2022 |
| `prostatecancerkirby` | prostate cancer | prostate | adult men | 3 |  | 2017 |
| `reedbmi` | BMI methylation score | whole blood, cord blood | all ages | 135 |  | 2020 |
| `replitali` | replicative history | cultured primary human cells | human cell cultures | 87 |  | 2022 |
| `replitalinorm` | replicative history | cultured fibroblasts | human cell cultures | 218 |  | 2022 |
| `sencultureage` | cellular senescence | cultured fibroblasts, cultured mesenchymal stromal cells | human cell cultures | 142 |  | 2026 |
| `senmortalityage` | mortality risk | whole blood | adults | 91 |  | 2026 |
| `twelvecelldeconvolutebloodepicbas` | basophil proportion | purified blood leukocytes | adults | 240 |  | 2022 |
| `twelvecelldeconvolutebloodepicbmem` | memory B cell proportion | purified blood leukocytes | adults | 240 |  | 2022 |
| `twelvecelldeconvolutebloodepicbnv` | naive B cell proportion | purified blood leukocytes | adults | 240 |  | 2022 |
| `twelvecelldeconvolutebloodepiccd4mem` | memory CD4+ T cell proportion | purified blood leukocytes | adults | 240 |  | 2022 |
| `twelvecelldeconvolutebloodepiccd4nv` | naive CD4+ T cell proportion | purified blood leukocytes | adults | 240 |  | 2022 |
| `twelvecelldeconvolutebloodepiccd8mem` | memory CD8+ T cell proportion | purified blood leukocytes | adults | 240 |  | 2022 |
| `twelvecelldeconvolutebloodepiccd8nv` | naive CD8+ T cell proportion | purified blood leukocytes | adults | 240 |  | 2022 |
| `twelvecelldeconvolutebloodepiceos` | eosinophil proportion | purified blood leukocytes | adults | 240 |  | 2022 |
| `twelvecelldeconvolutebloodepicmono` | monocyte proportion | purified blood leukocytes | adults | 240 |  | 2022 |
| `twelvecelldeconvolutebloodepicneu` | neutrophil proportion | purified blood leukocytes | adults | 240 |  | 2022 |
| `twelvecelldeconvolutebloodepicnk` | natural killer cell proportion | purified blood leukocytes | adults | 240 |  | 2022 |
| `twelvecelldeconvolutebloodepictreg` | regulatory T-cell proportion | purified blood leukocytes | adults | 240 |  | 2022 |
| `xchrom` | X-chromosome dosage | whole blood | adults | 453152 |  | 2021 |
| `ychrom` | Y-chromosome presence | whole blood | adults | 453152 |  | 2021 |
| `yingadaptage` | adaptive epigenetic age | whole blood | adults | 999 |  | 2024 |
| `yingdamage` | damaging epigenetic age | whole blood | adults | 1089 |  | 2024 |
| `zhangmortality` | mortality risk | whole blood | older adults | 10 |  | 2017 |
| `cpgptgrimage3` | biological age, mortality risk | whole blood | adults | 24 | age, grimage2timp1, grimage2packyrs, grimage2logcrp, grim… | 2024 |
| `cpgptpcgrimage3` | biological age, mortality risk | whole blood | adults | 31 | age, grimage2timp1, grimage2packyrs, grimage2logcrp, grim… | 2024 |
| `dnamfitage` | physical-fitness biological age | whole blood | adults | 1343 | female, age | 2023 |
| `dnamfitagegait` | gait speed | whole blood | adults | 111 | female | 2023 |
| `dnamfitagegrip` | grip strength | whole blood | adults | 183 | female | 2023 |
| `dnamfitagevo2max` | VO2max | whole blood | adults | 41 | age | 2023 |
| `grimage` | mortality risk | whole blood | adults | 1032 | female, age | 2019 |
| `grimage2` | mortality risk | whole blood | older adults | 1032 | female, age | 2022 |
| `grimage2adm` | adrenomedullin | whole blood | older adults | 187 | age | 2022 |
| `grimage2b2m` | beta-2-microglobulin | whole blood | older adults | 92 | age | 2022 |
| `grimage2cystatinc` | cystatin C | whole blood | older adults | 88 | age | 2022 |
| `grimage2gdf15` | GDF-15 | whole blood | older adults | 138 | age | 2022 |
| `grimage2loga1c` | hemoglobin A1c | whole blood | older adults | 87 | age | 2022 |
| `grimage2packyrs` | smoking exposure | whole blood | older adults | 173 | age | 2022 |
| `grimage2timp1` | TIMP-1 | whole blood | older adults | 43 | age | 2022 |
| `pcgrimage` | mortality risk | whole blood | adults | 78466 | female, age | 2022 |
| `pcgrimageadm` | adrenomedullin | whole blood | adults | 78466 | female, age | 2022 |
| `pcgrimageb2m` | beta-2-microglobulin | whole blood | adults | 78465 | age | 2022 |
| `pcgrimagecystatinc` | cystatin C | whole blood | adults | 78466 | female, age | 2022 |
| `pcgrimagegdf15` | GDF-15 | whole blood | adults | 78465 | age | 2022 |
| `pcgrimageleptin` | leptin | whole blood | adults | 78466 | female, age | 2022 |
| `pcgrimagepackyrs` | smoking exposure | whole blood | adults | 78466 | female, age | 2022 |
| `pcgrimagepai1` | PAI-1 | whole blood | adults | 78465 | female | 2022 |
| `pcgrimagetimp1` | TIMP-1 | whole blood | adults | 78466 | female, age | 2022 |

## Citations

- `abec`: Lee, Yunsung, et al. "Blood-based epigenetic estimators of chronological age in human adults using DNA methylation data from the Illumina MethylationEPIC array." BMC genomics 21 (2020): 1-13.
- `altumage`: de Lima Camillo, L.P., Lapierre, L.R. & Singh, R. A pan-tissue DNA-methylation epigenetic clock based on deep learning. npj Aging 8, 4 (2022).
- `cabec`: Lee, Yunsung, et al. "Blood-based epigenetic estimators of chronological age in human adults using DNA methylation data from the Illumina MethylationEPIC array." BMC genomics 21 (2020): 1-13.
- `corticalclock`: Shireby, G. L., et al. "Recalibrating the epigenetic clock: implications for assessing biological age in the human cortex." Brain 143.12 (2020): 3763-3775.
- `ctsliver`: Tong, Huige, et al. "Cell-type specific epigenetic clocks to quantify biological age at cell-type resolution." Aging 16 (2024): 14204–14237.
- `eabec`: Lee, Yunsung, et al. "Blood-based epigenetic estimators of chronological age in human adults using DNA methylation data from the Illumina MethylationEPIC array." BMC genomics 21 (2020): 1-13.
- `encen100`: Dec, Eric, et al. "Centenarian clocks: epigenetic clocks for validating claims of exceptional longevity." GeroScience 45 (2023): 1817–1835.
- `encen40`: Dec, Eric, et al. "Centenarian clocks: epigenetic clocks for validating claims of exceptional longevity." GeroScience 45 (2023): 1817–1835.
- `gliasin`: Tong, Huige, et al. "Cell-type specific epigenetic clocks to quantify biological age at cell-type resolution." Aging 16 (2024): 14204–14237.
- `han`: Han, Y., et al. “New targeted approaches for epigenetic age predictions.” BMC Biology 18, 71 (2020).
- `hannum`: Hannum, G., et al. “Genome-wide methylation profiles reveal quantitative views of human aging rates.” Molecular Cell 49(2), 359–367 (2013).
- `hep`: Tong, Huige, et al. "Cell-type specific epigenetic clocks to quantify biological age at cell-type resolution." Aging 16 (2024): 14204–14237.
- `horvath2013`: Horvath, S. DNA methylation age of human tissues and cell types. Genome Biology 14, R115 (2013).
- `intrinclock`: Tomusiak, A., et al. “Development of an epigenetic clock resistant to changes in immune cell composition.” Communications Biology 7, 934 (2024).
- `intrinclock370`: Tomusiak, A., et al. “Development of an epigenetic clock resistant to changes in immune cell composition.” Communications Biology 7, 934 (2024).
- `lin`: Lin, Q., et al. “DNA methylation levels at individual age-associated CpG sites can be indicative for life expectancy.” Aging 8(2): 394–401 (2016).
- `neusin`: Tong, Huige, et al. "Cell-type specific epigenetic clocks to quantify biological age at cell-type resolution." Aging 16 (2024): 13452–13504.
- `pcbrainage`: Thrush, K. L., Bennett, D. A., Gaiteri, C., Horvath, S., van Dyck, C. H., Higgins-Chen, A. T., and Levine, M. E. (2022). Aging the brain: multi-region methylation principal component based clock in the context of Alzheimer’s disease. Aging 14(14), 5641–5668.
- `pchannum`: Higgins-Chen, Albert T., et al. "A computational solution for bolstering reliability of epigenetic clocks: implications for clinical trials and longitudinal tracking." Nature Aging 2 (2022): 644–661.
- `pchorvath2013`: Higgins-Chen, Albert T., et al. "A computational solution for bolstering reliability of epigenetic clocks: implications for clinical trials and longitudinal tracking." Nature Aging 2 (2022): 644–661.
- `pcskinandblood`: Higgins-Chen, Albert T., et al. "A computational solution for bolstering reliability of epigenetic clocks: implications for clinical trials and longitudinal tracking." Nature Aging 2 (2022): 644–661.
- `pedbe`: McEwen, Lisa M., et al. "The PedBE clock accurately estimates DNA methylation age in pediatric buccal cells." Proceedings of the National Academy of Sciences 117 (2020): 23329–23335.
- `pipekelasticnet`: Pipek, Orsolya Anna, and István Csabai. "A revised multi-tissue, multi-platform epigenetic clock model for methylation array data." Journal of Mathematical Chemistry 61 (2023): 376–388.
- `pipekfilteredh`: Pipek, Orsolya Anna, and István Csabai. "A revised multi-tissue, multi-platform epigenetic clock model for methylation array data." Journal of Mathematical Chemistry 61 (2023): 376–388.
- `pipekretrainedh`: Pipek, Orsolya Anna, and István Csabai. "A revised multi-tissue, multi-platform epigenetic clock model for methylation array data." Journal of Mathematical Chemistry 61 (2023): 376–388.
- `retroelementagev1`: Ndhlovu, Lishomwa C., et al. "Retro-age: A unique epigenetic biomarker of aging captured by DNA methylation states of retroelements." Aging Cell 23 (2024): e14288.
- `retroelementagev2`: Ndhlovu, Lishomwa C., et al. "Retro-age: A unique epigenetic biomarker of aging captured by DNA methylation states of retroelements." Aging Cell 23 (2024): e14288.
- `senchronoage`: Kasamoto, K., Gibson, J., Moqri, M., Smith, R. & Higgins-Chen, A.T. DNA methylation signatures of cellular senescence are not reversed by senolytic treatment. Aging Cell 25, e70430 (2026).
- `skinandblood`: Horvath, S., et al. “Epigenetic clock for skin and blood cells applied to Hutchinson Gilford Progeria Syndrome and ex vivo studies.” Aging 10(7), 1758–1775 (2018).
- `stoch`: Tong, Huige, et al. "Quantifying the stochastic component of epigenetic aging." Nature Aging 4 (2024): 886–901.
- `stocp`: Tong, Huige, et al. "Quantifying the stochastic component of epigenetic aging." Nature Aging 4 (2024): 886–901.
- `stocz`: Tong, Huige, et al. "Quantifying the stochastic component of epigenetic aging." Nature Aging 4 (2024): 886–901.
- `vidalbralo`: Vidal-Bralo, Laura, Yolanda Lopez-Golan, and Antonio Gonzalez. "Simplified assay for epigenetic age estimation in whole blood of adults." Frontiers in Genetics 7 (2016): 126.
- `wu`: Wu, X., et al. “DNA methylation profile is a quantitative measure of biological aging in children.” Aging 11(22): 10031–10051 (2019).
- `yingcausage`: Ying, K., Liu, H., Tarkhov, A.E. et al. Causality-enriched epigenetic age uncouples damage and adaptation. Nature Aging 4, 231–246 (2024).
- `zhangblup`: Zhang, Q., Vallerga, C.L., Walker, R.M. et al. Improved precision of epigenetic clock estimates across tissues and its implication for biological ageing. Genome Medicine 11, 54 (2019).
- `zhangen`: Zhang, Q., Vallerga, C.L., Walker, R.M. et al. Improved precision of epigenetic clock estimates across tissues and its implication for biological ageing. Genome Medicine 11, 54 (2019).
- `dnamphenoage`: Levine, M. E., et al. "An epigenetic biomarker of aging for lifespan and healthspan." Aging 10.4 (2018): 573-591.
- `hrsinchphenoage`: Higgins-Chen, Albert T., et al. "A computational solution for bolstering reliability of epigenetic clocks: implications for clinical trials and longitudinal tracking." Nature Aging 2 (2022): 644–661.
- `pcphenoage`: Higgins-Chen, Albert T., et al. "A computational solution for bolstering reliability of epigenetic clocks: implications for clinical trials and longitudinal tracking." Nature Aging 2 (2022): 644–661.
- `weidner`: Weidner, C. I., Lin, Q., Koch, C. M., et al. (2014). Aging of blood can be tracked by DNA methylation changes at just three CpG sites. Genome Biology, 15, R24.
- `bohlin`: Bohlin, J., Håberg, S. E., Magnus, P. et al. Prediction of gestational age based on genome-wide differentially methylated regions. Genome Biology 17, 207 (2016).
- `epicga`: Haftorn, K. L. et al. An EPIC predictor of gestational age and its application to newborns conceived by assisted reproductive technologies. Clinical Epigenetics 13, 82 (2021).
- `knight`: Knight, A. K., et al. "An epigenetic clock for gestational age at birth based on blood methylation data." Genome Biology 17 (2016): 206.
- `leecontrol`: Lee, Y., et al. “Placental epigenetic clocks: estimating gestational age using placental DNA methylation levels.” Aging 11(12): 4238–4253 (2019).
- `leerefinedrobust`: Lee, Y., et al. “Placental epigenetic clocks: estimating gestational age using placental DNA methylation levels.” Aging 11(12): 4238–4253 (2019).
- `leerobust`: Lee, Y., et al. “Placental epigenetic clocks: estimating gestational age using placental DNA methylation levels.” Aging 11(12): 4238–4253 (2019).
- `mayne`: Mayne, Benjamin T., et al. "Accelerated placental aging in early onset preeclampsia pregnancies identified by DNA methylation." Epigenomics 9 (2017): 279–289.
- `celldrift`: Minteer, C. J., et al. (2023). More than bad luck: Cancer and aging are linked to replication-driven changes to the epigenome. Science Advances, 9(29), eadf4163.
- `epicmithyper`: Duran-Ferrer, M., et al. "The proliferative history shapes the DNA methylome of B-cell tumors and predicts clinical outcome." Nature Cancer 1 (2020): 1066-1081.
- `epicmithypo`: Duran-Ferrer, M., et al. "The proliferative history shapes the DNA methylome of B-cell tumors and predicts clinical outcome." Nature Cancer 1 (2020): 1066-1081.
- `epitoc1`: Yang, Zhen, et al. "Correlation of an epigenetic mitotic clock with cancer risk." Genome Biology 17 (2016): 205.
- `epitoc2`: Teschendorff, Andrew E. "A comparison of epigenetic mitotic-like clocks for cancer risk prediction." Genome Medicine 12 (2020): 56.
- `epitoc3`: Teschendorff, Andrew E. "A comparison of epigenetic mitotic-like clocks for cancer risk prediction." Genome Medicine 12 (2020): 56.
- `hypoclock`: Teschendorff, Andrew E. "A comparison of epigenetic mitotic-like clocks for cancer risk prediction." Genome Medicine 12 (2020): 56.
- `miage`: Youn, A., and Wang, S. (2018). The MiAge Calculator: a DNA methylation-based mitotic age calculator of human tissue types. Epigenetics, 13(2), 192–206.
- `stemtoc`: Zhu, Tianlei, et al. "An improved epigenetic counter to track mitotic age in cells." Nature Communications 15 (2024): 4211.
- `stemtocvitro`: Zhu, Tianlei, et al. "An improved epigenetic counter to track mitotic age in cells." Nature Communications 15 (2024): 4211.
- `adbahadosingh`: Bahado-Singh, R. O., Vishweswaraiah, S., Aydas, B., et al. (2021). Artificial intelligence and leukocyte epigenomics: Evaluation and prediction of late-onset Alzheimer's disease. PLOS ONE, 16(4), e0248375.
- `bocklandt`: Bocklandt, S., Lin, W., Sehl, M.E. et al. Epigenetic predictor of age. PLoS ONE 6, e14821 (2011).
- `cellpopage`: Lujan, Celia, et al. "An expedited screening platform for the discovery of anti-ageing compounds in vitro and in vivo." Genome Medicine 16 (2024): 85.
- `compil6`: Stevenson, A. J., et al. “Creating and Validating a DNA Methylation-Based Proxy for Interleukin-6.” The Journals of Gerontology: Series A 76(12): 2284–2292 (2021).
- `cvdwesterman`: Westerman, K. et al. Epigenomic assessment of cardiovascular disease risk and interactions with traditional risk metrics. Journal of the American Heart Association 9, e015299 (2020).
- `deconvolutebloodepicbcell`: Salas, L.A., Koestler, D.C., Butler, R.A. et al. An optimized library for reference-based deconvolution of whole-blood biospecimens assayed using the Illumina HumanMethylationEPIC BeadArray. Genome Biology 19, 64 (2018).
- `deconvolutebloodepiccd4tcell`: Salas, L.A., Koestler, D.C., Butler, R.A. et al. An optimized library for reference-based deconvolution of whole-blood biospecimens assayed using the Illumina HumanMethylationEPIC BeadArray. Genome Biology 19, 64 (2018).
- `deconvolutebloodepiccd8tcell`: Salas, L.A., Koestler, D.C., Butler, R.A. et al. An optimized library for reference-based deconvolution of whole-blood biospecimens assayed using the Illumina HumanMethylationEPIC BeadArray. Genome Biology 19, 64 (2018).
- `deconvolutebloodepicmonocyte`: Salas, L.A., Koestler, D.C., Butler, R.A. et al. An optimized library for reference-based deconvolution of whole-blood biospecimens assayed using the Illumina HumanMethylationEPIC BeadArray. Genome Biology 19, 64 (2018).
- `deconvolutebloodepicneutrophil`: Salas, L.A., Koestler, D.C., Butler, R.A. et al. An optimized library for reference-based deconvolution of whole-blood biospecimens assayed using the Illumina HumanMethylationEPIC BeadArray. Genome Biology 19, 64 (2018).
- `deconvolutebloodepicnkcell`: Salas, L.A., Koestler, D.C., Butler, R.A. et al. An optimized library for reference-based deconvolution of whole-blood biospecimens assayed using the Illumina HumanMethylationEPIC BeadArray. Genome Biology 19, 64 (2018).
- `depressionbarbu`: Barbu, Miruna C., et al. "Epigenetic prediction of major depressive disorder." Molecular Psychiatry 26.9 (2021): 5112-5123.
- `dnamfili`: Li, X. et al. Derivation and validation of an epigenetic frailty risk score in population-based cohorts of older adults. Nature Communications 13, 5269 (2022).
- `dnamic`: Fuentealba, M., Rouch, L., Guyonnet, S. et al. A blood-based epigenetic clock for intrinsic capacity predicts mortality and is associated with clinical, immunological and lifestyle factors. Nature Aging 5, 1207–1216 (2025).
- `dnamphenoagenonprc`: ['Rozenblit, M., et al. Evidence of accelerated epigenetic aging of breast tissues in patients with breast cancer is driven by CpGs associated with polycomb-related genes. Clinical Epigenetics 14, 30 (2022).', 'Levine, M. E., et al. "An epigenetic biomarker of aging for lifespan and healthspan." Aging 10.4 (2018): 573-591.']
- `dnamphenoageprc`: ['Rozenblit, M., et al. Evidence of accelerated epigenetic aging of breast tissues in patients with breast cancer is driven by CpGs associated with polycomb-related genes. Clinical Epigenetics 14, 30 (2022).', 'Levine, M. E., et al. "An epigenetic biomarker of aging for lifespan and healthspan." Aging 10.4 (2018): 573-591.']
- `dnamstress`: Jung, J., et al. “Additive Effects of Stress and Alcohol Exposure on Accelerated Epigenetic Aging in Alcohol Use Disorder.” Biological Psychiatry 93(4): 331–341 (2023).
- `dnamtl`: Lu, A. T., et al. "DNA methylation-based estimator of telomere length." Aging 11.16 (2019): 5895-5923.
- `downsyndrome`: Muskens, Ivo S., et al. "The genome-wide impact of trisomy 21 on DNA methylation and its implications for hematopoiesis." Nature Communications 12 (2021): 821.
- `dunedinpace`: Belsky, D. W., Caspi, A., Corcoran, D. L., et al. (2022). DunedinPACE, a DNA methylation biomarker of the pace of aging. eLife, 11, e73420.
- `dunedinpoam38`: Belsky, D.W., Caspi, A., Arseneault, L. et al. Quantification of the pace of biological aging in humans through a blood test, the DunedinPoAm DNA methylation algorithm. eLife 9, e54870 (2020).
- `garagnani`: Garagnani, Paolo, et al. "Methylation of ELOVL2 gene as a new epigenetic marker of age." Aging Cell 11.6 (2012): 1132-1134.
- `grimage2leptin`: Lu, A. T., et al. “DNA methylation GrimAge version 2.” Aging 14(23): 9484–9549 (2022).
- `grimage2logcrp`: Lu, A. T., et al. “DNA methylation GrimAge version 2.” Aging 14(23): 9484–9549 (2022).
- `grimage2pai1`: Lu, A. T., et al. “DNA methylation GrimAge version 2.” Aging 14(23): 9484–9549 (2022).
- `hepatoxu`: Xu, R.-H., et al. “Circulating tumour DNA methylation markers for diagnosis and prognosis of hepatocellular carcinoma.” Nature Materials 16: 1155–1161 (2017).
- `mccartneyalcohol`: McCartney, D. L., et al. “Epigenetic prediction of complex traits and death.” Genome Biology 19, 136 (2018).
- `mccartneybmi`: McCartney, D. L., et al. “Epigenetic prediction of complex traits and death.” Genome Biology 19, 136 (2018).
- `mccartneybodyfat`: McCartney, D. L., et al. “Epigenetic prediction of complex traits and death.” Genome Biology 19, 136 (2018).
- `mccartneyeducation`: McCartney, D. L., et al. “Epigenetic prediction of complex traits and death.” Genome Biology 19, 136 (2018).
- `mccartneyhdlcholesterol`: McCartney, D. L., et al. “Epigenetic prediction of complex traits and death.” Genome Biology 19, 136 (2018).
- `mccartneyldlcholesterol`: McCartney, D. L., et al. “Epigenetic prediction of complex traits and death.” Genome Biology 19, 136 (2018).
- `mccartneysmoking`: McCartney, D. L., et al. “Epigenetic prediction of complex traits and death.” Genome Biology 19, 136 (2018).
- `mccartneytotalcholesterol`: McCartney, D. L., et al. “Epigenetic prediction of complex traits and death.” Genome Biology 19, 136 (2018).
- `mccartneytotalhdlratio`: McCartney, D. L., et al. “Epigenetic prediction of complex traits and death.” Genome Biology 19, 136 (2018).
- `mccartneywhr`: McCartney, D. L., et al. “Epigenetic prediction of complex traits and death.” Genome Biology 19, 136 (2018).
- `pcdnamtl`: Higgins-Chen, Albert T., et al. "A computational solution for bolstering reliability of epigenetic clocks: implications for clinical trials and longitudinal tracking." Nature Aging 2 (2022): 644–661.
- `prostatecancerkirby`: Kirby, M. K., et al. “Genome-wide DNA methylation measurements in prostate tissues uncovers novel prostate cancer diagnostic biomarkers and transcription factor binding patterns.” BMC Cancer 17: 273 (2017).
- `reedbmi`: Reed, Zoe E., et al. "The association of DNA methylation with body mass index: distinguishing between predictors and biomarkers." Clinical Epigenetics 12 (2020): 50.
- `replitali`: Endicott, J.L., Nolte, P.A., Shen, H. & Laird, P.W. Cell division drives DNA methylation loss in late-replicating domains in primary human cells. Nature Communications 13, 6659 (2022).
- `replitalinorm`: Endicott, J.L., Nolte, P.A., Shen, H. & Laird, P.W. Cell division drives DNA methylation loss in late-replicating domains in primary human cells. Nature Communications 13, 6659 (2022).
- `sencultureage`: Kasamoto, K., Gibson, J., Moqri, M., Smith, R. & Higgins-Chen, A.T. DNA methylation signatures of cellular senescence are not reversed by senolytic treatment. Aging Cell 25, e70430 (2026).
- `senmortalityage`: Kasamoto, K., Gibson, J., Moqri, M., Smith, R. & Higgins-Chen, A.T. DNA methylation signatures of cellular senescence are not reversed by senolytic treatment. Aging Cell 25, e70430 (2026).
- `twelvecelldeconvolutebloodepicbas`: Salas, L.A., Zhang, Z., Koestler, D.C. et al. Enhanced cell deconvolution of peripheral blood using DNA methylation for high-resolution immune profiling. Nature Communications 13, 761 (2022).
- `twelvecelldeconvolutebloodepicbmem`: Salas, L.A., Zhang, Z., Koestler, D.C. et al. Enhanced cell deconvolution of peripheral blood using DNA methylation for high-resolution immune profiling. Nature Communications 13, 761 (2022).
- `twelvecelldeconvolutebloodepicbnv`: Salas, L.A., Zhang, Z., Koestler, D.C. et al. Enhanced cell deconvolution of peripheral blood using DNA methylation for high-resolution immune profiling. Nature Communications 13, 761 (2022).
- `twelvecelldeconvolutebloodepiccd4mem`: Salas, L.A., Zhang, Z., Koestler, D.C. et al. Enhanced cell deconvolution of peripheral blood using DNA methylation for high-resolution immune profiling. Nature Communications 13, 761 (2022).
- `twelvecelldeconvolutebloodepiccd4nv`: Salas, L.A., Zhang, Z., Koestler, D.C. et al. Enhanced cell deconvolution of peripheral blood using DNA methylation for high-resolution immune profiling. Nature Communications 13, 761 (2022).
- `twelvecelldeconvolutebloodepiccd8mem`: Salas, L.A., Zhang, Z., Koestler, D.C. et al. Enhanced cell deconvolution of peripheral blood using DNA methylation for high-resolution immune profiling. Nature Communications 13, 761 (2022).
- `twelvecelldeconvolutebloodepiccd8nv`: Salas, L.A., Zhang, Z., Koestler, D.C. et al. Enhanced cell deconvolution of peripheral blood using DNA methylation for high-resolution immune profiling. Nature Communications 13, 761 (2022).
- `twelvecelldeconvolutebloodepiceos`: Salas, L.A., Zhang, Z., Koestler, D.C. et al. Enhanced cell deconvolution of peripheral blood using DNA methylation for high-resolution immune profiling. Nature Communications 13, 761 (2022).
- `twelvecelldeconvolutebloodepicmono`: Salas, L.A., Zhang, Z., Koestler, D.C. et al. Enhanced cell deconvolution of peripheral blood using DNA methylation for high-resolution immune profiling. Nature Communications 13, 761 (2022).
- `twelvecelldeconvolutebloodepicneu`: Salas, L.A., Zhang, Z., Koestler, D.C. et al. Enhanced cell deconvolution of peripheral blood using DNA methylation for high-resolution immune profiling. Nature Communications 13, 761 (2022).
- `twelvecelldeconvolutebloodepicnk`: Salas, L.A., Zhang, Z., Koestler, D.C. et al. Enhanced cell deconvolution of peripheral blood using DNA methylation for high-resolution immune profiling. Nature Communications 13, 761 (2022).
- `twelvecelldeconvolutebloodepictreg`: Salas, L.A., Zhang, Z., Koestler, D.C. et al. Enhanced cell deconvolution of peripheral blood using DNA methylation for high-resolution immune profiling. Nature Communications 13, 761 (2022).
- `xchrom`: Wang, Y. et al. DNA methylation-based sex classifier to predict sex and identify sex chromosome aneuploidy. BMC Genomics 22, 484 (2021).
- `ychrom`: Wang, Y. et al. DNA methylation-based sex classifier to predict sex and identify sex chromosome aneuploidy. BMC Genomics 22, 484 (2021).
- `yingadaptage`: Ying, K., Liu, H., Tarkhov, A.E. et al. Causality-enriched epigenetic age uncouples damage and adaptation. Nature Aging 4, 231–246 (2024).
- `yingdamage`: Ying, K., Liu, H., Tarkhov, A.E. et al. Causality-enriched epigenetic age uncouples damage and adaptation. Nature Aging 4, 231–246 (2024).
- `zhangmortality`: Zhang, Y., Wilson, R., Heiss, J. et al. DNA methylation signatures in peripheral blood strongly predict all-cause mortality. Nature Communications 8, 14617 (2017).
- `cpgptgrimage3`: de Lima Camillo, L. P., Sehgal, R., Armstrong, J., et al. CpGPT: a foundation model for DNA methylation. bioRxiv 2024.10.24.619766 (2024).
- `cpgptpcgrimage3`: de Lima Camillo, L. P., Sehgal, R., Armstrong, J., et al. CpGPT: a foundation model for DNA methylation. bioRxiv 2024.10.24.619766 (2024).
- `dnamfitage`: McGreevy, K. M., et al. “DNAmFitAge: biological age indicator incorporating physical fitness.” Aging 15(10): 3904–3938 (2023).
- `dnamfitagegait`: McGreevy, K. M., et al. “DNAmFitAge: biological age indicator incorporating physical fitness.” Aging 15(10): 3904–3938 (2023).
- `dnamfitagegrip`: McGreevy, K. M., et al. “DNAmFitAge: biological age indicator incorporating physical fitness.” Aging 15(10): 3904–3938 (2023).
- `dnamfitagevo2max`: McGreevy, K. M., et al. “DNAmFitAge: biological age indicator incorporating physical fitness.” Aging 15(10): 3904–3938 (2023).
- `grimage`: Lu, Ake T., et al. "DNA methylation GrimAge strongly predicts lifespan and healthspan." Aging (Albany NY) 11 (2019): 303–327.
- `grimage2`: Lu, A. T., et al. “DNA methylation GrimAge version 2.” Aging 14(23): 9484–9549 (2022).
- `grimage2adm`: Lu, A. T., et al. “DNA methylation GrimAge version 2.” Aging 14(23): 9484–9549 (2022).
- `grimage2b2m`: Lu, A. T., et al. “DNA methylation GrimAge version 2.” Aging 14(23): 9484–9549 (2022).
- `grimage2cystatinc`: Lu, A. T., et al. “DNA methylation GrimAge version 2.” Aging 14(23): 9484–9549 (2022).
- `grimage2gdf15`: Lu, A. T., et al. “DNA methylation GrimAge version 2.” Aging 14(23): 9484–9549 (2022).
- `grimage2loga1c`: Lu, A. T., et al. “DNA methylation GrimAge version 2.” Aging 14(23): 9484–9549 (2022).
- `grimage2packyrs`: Lu, A. T., et al. “DNA methylation GrimAge version 2.” Aging 14(23): 9484–9549 (2022).
- `grimage2timp1`: Lu, A. T., et al. “DNA methylation GrimAge version 2.” Aging 14(23): 9484–9549 (2022).
- `pcgrimage`: Higgins-Chen, Albert T., et al. "A computational solution for bolstering reliability of epigenetic clocks: implications for clinical trials and longitudinal tracking." Nature Aging 2 (2022): 644–661.
- `pcgrimageadm`: Higgins-Chen, Albert T., et al. "A computational solution for bolstering reliability of epigenetic clocks: implications for clinical trials and longitudinal tracking." Nature Aging 2 (2022): 644–661.
- `pcgrimageb2m`: Higgins-Chen, Albert T., et al. "A computational solution for bolstering reliability of epigenetic clocks: implications for clinical trials and longitudinal tracking." Nature Aging 2 (2022): 644–661.
- `pcgrimagecystatinc`: Higgins-Chen, Albert T., et al. "A computational solution for bolstering reliability of epigenetic clocks: implications for clinical trials and longitudinal tracking." Nature Aging 2 (2022): 644–661.
- `pcgrimagegdf15`: Higgins-Chen, Albert T., et al. "A computational solution for bolstering reliability of epigenetic clocks: implications for clinical trials and longitudinal tracking." Nature Aging 2 (2022): 644–661.
- `pcgrimageleptin`: Higgins-Chen, Albert T., et al. "A computational solution for bolstering reliability of epigenetic clocks: implications for clinical trials and longitudinal tracking." Nature Aging 2 (2022): 644–661.
- `pcgrimagepackyrs`: Higgins-Chen, Albert T., et al. "A computational solution for bolstering reliability of epigenetic clocks: implications for clinical trials and longitudinal tracking." Nature Aging 2 (2022): 644–661.
- `pcgrimagepai1`: Higgins-Chen, Albert T., et al. "A computational solution for bolstering reliability of epigenetic clocks: implications for clinical trials and longitudinal tracking." Nature Aging 2 (2022): 644–661.
- `pcgrimagetimp1`: Higgins-Chen, Albert T., et al. "A computational solution for bolstering reliability of epigenetic clocks: implications for clinical trials and longitudinal tracking." Nature Aging 2 (2022): 644–661.
