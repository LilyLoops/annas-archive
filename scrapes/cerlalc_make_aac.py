import pymysql
import orjson
import shortuuid
import datetime
import isbnlib
import tqdm

# Scrubbing:

# ALTER TABLE cerlalc_bolivia.editores DROP COLUMN `documento`, DROP COLUMN `password`, DROP COLUMN `passwordplano`, DROP COLUMN `direccion`, DROP COLUMN `telefono`, DROP COLUMN `fax`, DROP COLUMN `mail`, DROP COLUMN `respisbn_telefono`, DROP COLUMN `respisbn_email`, DROP COLUMN `mailfacturacion`, DROP COLUMN `imagen`, DROP COLUMN `mmdd_imagen_filename`, DROP COLUMN `mmdd_imagen_filetype`, DROP COLUMN `mmdd_imagen_filesize`;
# ALTER TABLE cerlalc_chile.editores DROP COLUMN `documento`, DROP COLUMN `password`, DROP COLUMN `passwordplano`, DROP COLUMN `direccion`, DROP COLUMN `telefono`, DROP COLUMN `fax`, DROP COLUMN `mail`, DROP COLUMN `respisbn_telefono`, DROP COLUMN `respisbn_email`, DROP COLUMN `mailfacturacion`, DROP COLUMN `imagen`, DROP COLUMN `mmdd_imagen_filename`, DROP COLUMN `mmdd_imagen_filetype`, DROP COLUMN `mmdd_imagen_filesize`;
# ALTER TABLE cerlalc_costa_rica.editores DROP COLUMN `documento`, DROP COLUMN `password`, DROP COLUMN `passwordplano`, DROP COLUMN `direccion`, DROP COLUMN `telefono`, DROP COLUMN `fax`, DROP COLUMN `mail`, DROP COLUMN `respisbn_telefono`, DROP COLUMN `respisbn_email`, DROP COLUMN `mailfacturacion`, DROP COLUMN `imagen`, DROP COLUMN `mmdd_imagen_filename`, DROP COLUMN `mmdd_imagen_filetype`, DROP COLUMN `mmdd_imagen_filesize`;
# ALTER TABLE cerlalc_ecuador.editores DROP COLUMN `documento`, DROP COLUMN `password`, DROP COLUMN `passwordplano`, DROP COLUMN `direccion`, DROP COLUMN `telefono`, DROP COLUMN `fax`, DROP COLUMN `mail`, DROP COLUMN `respisbn_telefono`, DROP COLUMN `respisbn_email`, DROP COLUMN `mailfacturacion`, DROP COLUMN `imagen`, DROP COLUMN `mmdd_imagen_filename`, DROP COLUMN `mmdd_imagen_filetype`, DROP COLUMN `mmdd_imagen_filesize`;
# ALTER TABLE cerlalc_guatemala.editores DROP COLUMN `documento`, DROP COLUMN `password`, DROP COLUMN `passwordplano`, DROP COLUMN `direccion`, DROP COLUMN `telefono`, DROP COLUMN `fax`, DROP COLUMN `mail`, DROP COLUMN `respisbn_telefono`, DROP COLUMN `respisbn_email`, DROP COLUMN `mailfacturacion`, DROP COLUMN `imagen`, DROP COLUMN `mmdd_imagen_filename`, DROP COLUMN `mmdd_imagen_filetype`, DROP COLUMN `mmdd_imagen_filesize`;
# ALTER TABLE cerlalc_honduras.editores DROP COLUMN `documento`, DROP COLUMN `password`, DROP COLUMN `passwordplano`, DROP COLUMN `direccion`, DROP COLUMN `telefono`, DROP COLUMN `fax`, DROP COLUMN `mail`, DROP COLUMN `respisbn_telefono`, DROP COLUMN `respisbn_email`, DROP COLUMN `mailfacturacion`, DROP COLUMN `imagen`, DROP COLUMN `mmdd_imagen_filename`, DROP COLUMN `mmdd_imagen_filetype`, DROP COLUMN `mmdd_imagen_filesize`;
# ALTER TABLE cerlalc_mexico.editores DROP COLUMN `documento`, DROP COLUMN `password`, DROP COLUMN `passwordplano`, DROP COLUMN `direccion`, DROP COLUMN `telefono`, DROP COLUMN `fax`, DROP COLUMN `mail`, DROP COLUMN `respisbn_telefono`, DROP COLUMN `respisbn_email`, DROP COLUMN `mailfacturacion`, DROP COLUMN `imagen`, DROP COLUMN `mmdd_imagen_filename`, DROP COLUMN `mmdd_imagen_filetype`, DROP COLUMN `mmdd_imagen_filesize`;
# ALTER TABLE cerlalc_panama.editores DROP COLUMN `documento`, DROP COLUMN `password`, DROP COLUMN `passwordplano`, DROP COLUMN `direccion`, DROP COLUMN `telefono`, DROP COLUMN `fax`, DROP COLUMN `mail`, DROP COLUMN `respisbn_telefono`, DROP COLUMN `respisbn_email`, DROP COLUMN `mailfacturacion`, DROP COLUMN `imagen`, DROP COLUMN `mmdd_imagen_filename`, DROP COLUMN `mmdd_imagen_filetype`, DROP COLUMN `mmdd_imagen_filesize`;
# ALTER TABLE cerlalc_paraguay.editores DROP COLUMN `documento`, DROP COLUMN `password`, DROP COLUMN `passwordplano`, DROP COLUMN `direccion`, DROP COLUMN `telefono`, DROP COLUMN `fax`, DROP COLUMN `mail`, DROP COLUMN `respisbn_telefono`, DROP COLUMN `respisbn_email`, DROP COLUMN `mailfacturacion`, DROP COLUMN `imagen`, DROP COLUMN `mmdd_imagen_filename`, DROP COLUMN `mmdd_imagen_filetype`, DROP COLUMN `mmdd_imagen_filesize`;
# ALTER TABLE cerlalc_peru.editores DROP COLUMN `documento`, DROP COLUMN `password`, DROP COLUMN `passwordplano`, DROP COLUMN `direccion`, DROP COLUMN `telefono`, DROP COLUMN `fax`, DROP COLUMN `mail`, DROP COLUMN `respisbn_telefono`, DROP COLUMN `respisbn_email`, DROP COLUMN `mailfacturacion`, DROP COLUMN `imagen`, DROP COLUMN `mmdd_imagen_filename`, DROP COLUMN `mmdd_imagen_filetype`, DROP COLUMN `mmdd_imagen_filesize`;
# ALTER TABLE cerlalc_republica_dominicana.editores DROP COLUMN `documento`, DROP COLUMN `password`, DROP COLUMN `passwordplano`, DROP COLUMN `direccion`, DROP COLUMN `telefono`, DROP COLUMN `fax`, DROP COLUMN `mail`, DROP COLUMN `respisbn_telefono`, DROP COLUMN `respisbn_email`, DROP COLUMN `mailfacturacion`, DROP COLUMN `imagen`, DROP COLUMN `mmdd_imagen_filename`, DROP COLUMN `mmdd_imagen_filetype`, DROP COLUMN `mmdd_imagen_filesize`;
# ALTER TABLE cerlalc_uruguay.editores DROP COLUMN `documento`, DROP COLUMN `password`, DROP COLUMN `passwordplano`, DROP COLUMN `direccion`, DROP COLUMN `telefono`, DROP COLUMN `fax`, DROP COLUMN `mail`, DROP COLUMN `respisbn_telefono`, DROP COLUMN `respisbn_email`, DROP COLUMN `mailfacturacion`, DROP COLUMN `imagen`, DROP COLUMN `mmdd_imagen_filename`, DROP COLUMN `mmdd_imagen_filetype`, DROP COLUMN `mmdd_imagen_filesize`;

# DROP TABLE IF EXISTS cerlalc_bolivia.pel_pagos;
# ALTER TABLE cerlalc_bolivia.log DROP COLUMN `ip`;
# ALTER TABLE cerlalc_bolivia.log2 DROP COLUMN `ip`;
# DELETE FROM cerlalc_bolivia.log WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DELETE FROM cerlalc_bolivia.log2 WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DROP TABLE IF EXISTS cerlalc_chile.pel_pagos;
# ALTER TABLE cerlalc_chile.log DROP COLUMN `ip`;
# ALTER TABLE cerlalc_chile.log2 DROP COLUMN `ip`;
# DELETE FROM cerlalc_chile.log WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DELETE FROM cerlalc_chile.log2 WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DROP TABLE IF EXISTS cerlalc_costa_rica.pel_pagos;
# ALTER TABLE cerlalc_costa_rica.log DROP COLUMN `ip`;
# ALTER TABLE cerlalc_costa_rica.log2 DROP COLUMN `ip`;
# DELETE FROM cerlalc_costa_rica.log WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DELETE FROM cerlalc_costa_rica.log2 WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DROP TABLE IF EXISTS cerlalc_ecuador.pel_pagos;
# ALTER TABLE cerlalc_ecuador.log DROP COLUMN `ip`;
# ALTER TABLE cerlalc_ecuador.log2 DROP COLUMN `ip`;
# DELETE FROM cerlalc_ecuador.log WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DELETE FROM cerlalc_ecuador.log2 WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DROP TABLE IF EXISTS cerlalc_guatemala.pel_pagos;
# ALTER TABLE cerlalc_guatemala.log DROP COLUMN `ip`;
# ALTER TABLE cerlalc_guatemala.log2 DROP COLUMN `ip`;
# DELETE FROM cerlalc_guatemala.log WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DELETE FROM cerlalc_guatemala.log2 WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DROP TABLE IF EXISTS cerlalc_honduras.pel_pagos;
# ALTER TABLE cerlalc_honduras.log DROP COLUMN `ip`;
# ALTER TABLE cerlalc_honduras.log2 DROP COLUMN `ip`;
# DELETE FROM cerlalc_honduras.log WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DELETE FROM cerlalc_honduras.log2 WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DROP TABLE IF EXISTS cerlalc_mexico.pel_pagos;
# ALTER TABLE cerlalc_mexico.log DROP COLUMN `ip`;
# ALTER TABLE cerlalc_mexico.log2 DROP COLUMN `ip`;
# DELETE FROM cerlalc_mexico.log WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DELETE FROM cerlalc_mexico.log2 WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DROP TABLE IF EXISTS cerlalc_panama.pel_pagos;
# ALTER TABLE cerlalc_panama.log DROP COLUMN `ip`;
# ALTER TABLE cerlalc_panama.log2 DROP COLUMN `ip`;
# DELETE FROM cerlalc_panama.log WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DELETE FROM cerlalc_panama.log2 WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DROP TABLE IF EXISTS cerlalc_paraguay.pel_pagos;
# ALTER TABLE cerlalc_paraguay.log DROP COLUMN `ip`;
# ALTER TABLE cerlalc_paraguay.log2 DROP COLUMN `ip`;
# DELETE FROM cerlalc_paraguay.log WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DELETE FROM cerlalc_paraguay.log2 WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DROP TABLE IF EXISTS cerlalc_peru.pel_pagos;
# ALTER TABLE cerlalc_peru.log DROP COLUMN `ip`;
# ALTER TABLE cerlalc_peru.log2 DROP COLUMN `ip`;
# DELETE FROM cerlalc_peru.log WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DELETE FROM cerlalc_peru.log2 WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DROP TABLE IF EXISTS cerlalc_republica_dominicana.pel_pagos;
# ALTER TABLE cerlalc_republica_dominicana.log DROP COLUMN `ip`;
# ALTER TABLE cerlalc_republica_dominicana.log2 DROP COLUMN `ip`;
# DELETE FROM cerlalc_republica_dominicana.log WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DELETE FROM cerlalc_republica_dominicana.log2 WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DROP TABLE IF EXISTS cerlalc_uruguay.pel_pagos;
# ALTER TABLE cerlalc_uruguay.log DROP COLUMN `ip`;
# ALTER TABLE cerlalc_uruguay.log2 DROP COLUMN `ip`;
# DELETE FROM cerlalc_uruguay.log WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');
# DELETE FROM cerlalc_uruguay.log2 WHERE tabla = 'editores' AND campo IN ('documento','password','passwordplano','direccion','telefono','fax','mail','respisbn_telefono','respisbn_email','mailfacturacion','imagen','mmdd_imagen_filename','mmdd_imagen_filetype','mmdd_imagen_filesize');


# ALTER TABLE cerlalc_bolivia.usuarios DROP COLUMN `login`, DROP COLUMN `password`, DROP COLUMN `email`;
# ALTER TABLE cerlalc_chile.usuarios DROP COLUMN `login`, DROP COLUMN `password`, DROP COLUMN `email`;
# ALTER TABLE cerlalc_costa_rica.usuarios DROP COLUMN `login`, DROP COLUMN `password`, DROP COLUMN `email`;
# ALTER TABLE cerlalc_ecuador.usuarios DROP COLUMN `login`, DROP COLUMN `password`, DROP COLUMN `email`;
# ALTER TABLE cerlalc_guatemala.usuarios DROP COLUMN `login`, DROP COLUMN `password`, DROP COLUMN `email`;
# ALTER TABLE cerlalc_honduras.usuarios DROP COLUMN `login`, DROP COLUMN `password`, DROP COLUMN `email`;
# ALTER TABLE cerlalc_mexico.usuarios DROP COLUMN `login`, DROP COLUMN `password`, DROP COLUMN `email`;
# ALTER TABLE cerlalc_panama.usuarios DROP COLUMN `login`, DROP COLUMN `password`, DROP COLUMN `email`;
# ALTER TABLE cerlalc_paraguay.usuarios DROP COLUMN `login`, DROP COLUMN `password`, DROP COLUMN `email`;
# ALTER TABLE cerlalc_peru.usuarios DROP COLUMN `login`, DROP COLUMN `password`, DROP COLUMN `email`;
# ALTER TABLE cerlalc_republica_dominicana.usuarios DROP COLUMN `login`, DROP COLUMN `password`, DROP COLUMN `email`;
# ALTER TABLE cerlalc_uruguay.usuarios DROP COLUMN `login`, DROP COLUMN `password`, DROP COLUMN `email`;
# DELETE FROM cerlalc_bolivia.log WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_bolivia.log2 WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_chile.log WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_chile.log2 WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_costa_rica.log WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_costa_rica.log2 WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_ecuador.log WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_ecuador.log2 WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_guatemala.log WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_guatemala.log2 WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_honduras.log WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_honduras.log2 WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_mexico.log WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_mexico.log2 WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_panama.log WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_panama.log2 WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_paraguay.log WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_paraguay.log2 WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_peru.log WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_peru.log2 WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_republica_dominicana.log WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_republica_dominicana.log2 WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_uruguay.log WHERE tabla = 'usuarios' AND campo IN ('login','password','email');
# DELETE FROM cerlalc_uruguay.log2 WHERE tabla = 'usuarios' AND campo IN ('login','password','email');

# DELETE FROM cerlalc_bolivia.log WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_bolivia.log2 WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_chile.log WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_chile.log2 WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_costa_rica.log WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_costa_rica.log2 WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_ecuador.log WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_ecuador.log2 WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_guatemala.log WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_guatemala.log2 WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_honduras.log WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_honduras.log2 WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_mexico.log WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_mexico.log2 WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_panama.log WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_panama.log2 WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_paraguay.log WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_paraguay.log2 WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_peru.log WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_peru.log2 WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_republica_dominicana.log WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_republica_dominicana.log2 WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_uruguay.log WHERE tabla = 'editores' AND campo IS NULL;
# DELETE FROM cerlalc_uruguay.log2 WHERE tabla = 'editores' AND campo IS NULL;

# DROP TABLE IF EXISTS cerlalc_bolivia.tmp_tablas;
# DROP TABLE IF EXISTS cerlalc_chile.tmp_tablas;
# DROP TABLE IF EXISTS cerlalc_costa_rica.tmp_tablas;
# DROP TABLE IF EXISTS cerlalc_ecuador.tmp_tablas;
# DROP TABLE IF EXISTS cerlalc_guatemala.tmp_tablas;
# DROP TABLE IF EXISTS cerlalc_honduras.tmp_tablas;
# DROP TABLE IF EXISTS cerlalc_mexico.tmp_tablas;
# DROP TABLE IF EXISTS cerlalc_panama.tmp_tablas;
# DROP TABLE IF EXISTS cerlalc_paraguay.tmp_tablas;
# DROP TABLE IF EXISTS cerlalc_peru.tmp_tablas;
# DROP TABLE IF EXISTS cerlalc_republica_dominicana.tmp_tablas;
# DROP TABLE IF EXISTS cerlalc_uruguay.tmp_tablas;

# DELETE FROM cerlalc_bolivia.log WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_bolivia.log2 WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_chile.log WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_chile.log2 WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_costa_rica.log WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_costa_rica.log2 WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_ecuador.log WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_ecuador.log2 WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_guatemala.log WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_guatemala.log2 WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_honduras.log WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_honduras.log2 WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_mexico.log WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_mexico.log2 WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_panama.log WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_panama.log2 WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_paraguay.log WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_paraguay.log2 WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_peru.log WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_peru.log2 WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_republica_dominicana.log WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_republica_dominicana.log2 WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_uruguay.log WHERE tabla = 'editor_autor' AND campo IS NULL;
# DELETE FROM cerlalc_uruguay.log2 WHERE tabla = 'editor_autor' AND campo IS NULL;

# ALTER TABLE cerlalc_bolivia.editor_autor DROP COLUMN `edi_nic`, DROP COLUMN `edi_tel`, DROP COLUMN `edi_fax`, DROP COLUMN `edi_postal`, DROP COLUMN `edi_mail`, DROP COLUMN `edi_pass`;
# ALTER TABLE cerlalc_chile.editor_autor DROP COLUMN `edi_nic`, DROP COLUMN `edi_tel`, DROP COLUMN `edi_fax`, DROP COLUMN `edi_postal`, DROP COLUMN `edi_mail`, DROP COLUMN `edi_pass`;
# ALTER TABLE cerlalc_costa_rica.editor_autor DROP COLUMN `edi_nic`, DROP COLUMN `edi_tel`, DROP COLUMN `edi_fax`, DROP COLUMN `edi_postal`, DROP COLUMN `edi_mail`, DROP COLUMN `edi_pass`;
# ALTER TABLE cerlalc_ecuador.editor_autor DROP COLUMN `edi_nic`, DROP COLUMN `edi_tel`, DROP COLUMN `edi_fax`, DROP COLUMN `edi_postal`, DROP COLUMN `edi_mail`, DROP COLUMN `edi_pass`;
# ALTER TABLE cerlalc_guatemala.editor_autor DROP COLUMN `edi_nic`, DROP COLUMN `edi_tel`, DROP COLUMN `edi_fax`, DROP COLUMN `edi_postal`, DROP COLUMN `edi_mail`, DROP COLUMN `edi_pass`;
# ALTER TABLE cerlalc_honduras.editor_autor DROP COLUMN `edi_nic`, DROP COLUMN `edi_tel`, DROP COLUMN `edi_fax`, DROP COLUMN `edi_postal`, DROP COLUMN `edi_mail`, DROP COLUMN `edi_pass`;
# ALTER TABLE cerlalc_mexico.editor_autor DROP COLUMN `edi_nic`, DROP COLUMN `edi_tel`, DROP COLUMN `edi_fax`, DROP COLUMN `edi_postal`, DROP COLUMN `edi_mail`, DROP COLUMN `edi_pass`;
# ALTER TABLE cerlalc_panama.editor_autor DROP COLUMN `edi_nic`, DROP COLUMN `edi_tel`, DROP COLUMN `edi_fax`, DROP COLUMN `edi_postal`, DROP COLUMN `edi_mail`, DROP COLUMN `edi_pass`;
# ALTER TABLE cerlalc_paraguay.editor_autor DROP COLUMN `edi_nic`, DROP COLUMN `edi_tel`, DROP COLUMN `edi_fax`, DROP COLUMN `edi_postal`, DROP COLUMN `edi_mail`, DROP COLUMN `edi_pass`;
# ALTER TABLE cerlalc_peru.editor_autor DROP COLUMN `edi_nic`, DROP COLUMN `edi_tel`, DROP COLUMN `edi_fax`, DROP COLUMN `edi_postal`, DROP COLUMN `edi_mail`, DROP COLUMN `edi_pass`;
# ALTER TABLE cerlalc_republica_dominicana.editor_autor DROP COLUMN `edi_nic`, DROP COLUMN `edi_tel`, DROP COLUMN `edi_fax`, DROP COLUMN `edi_postal`, DROP COLUMN `edi_mail`, DROP COLUMN `edi_pass`;
# ALTER TABLE cerlalc_uruguay.editor_autor DROP COLUMN `edi_nic`, DROP COLUMN `edi_tel`, DROP COLUMN `edi_fax`, DROP COLUMN `edi_postal`, DROP COLUMN `edi_mail`, DROP COLUMN `edi_pass`;
# DELETE FROM cerlalc_bolivia.log WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_bolivia.log2 WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_chile.log WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_chile.log2 WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_costa_rica.log WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_costa_rica.log2 WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_ecuador.log WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_ecuador.log2 WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_guatemala.log WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_guatemala.log2 WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_honduras.log WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_honduras.log2 WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_mexico.log WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_mexico.log2 WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_panama.log WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_panama.log2 WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_paraguay.log WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_paraguay.log2 WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_peru.log WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_peru.log2 WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_republica_dominicana.log WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_republica_dominicana.log2 WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_uruguay.log WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');
# DELETE FROM cerlalc_uruguay.log2 WHERE tabla = 'editor_autor' AND campo IN ('edi_nic','edi_tel','edi_fax','edi_postal','edi_mail','edi_pass');

# ALTER TABLE cerlalc_bolivia.colaboradores DROP COLUMN `documento`, DROP COLUMN `mail`;
# ALTER TABLE cerlalc_chile.colaboradores DROP COLUMN `documento`, DROP COLUMN `mail`;
# ALTER TABLE cerlalc_costa_rica.colaboradores DROP COLUMN `documento`, DROP COLUMN `mail`;
# ALTER TABLE cerlalc_ecuador.colaboradores DROP COLUMN `documento`, DROP COLUMN `mail`;
# ALTER TABLE cerlalc_guatemala.colaboradores DROP COLUMN `documento`, DROP COLUMN `mail`;
# ALTER TABLE cerlalc_honduras.colaboradores DROP COLUMN `documento`, DROP COLUMN `mail`;
# ALTER TABLE cerlalc_mexico.colaboradores DROP COLUMN `documento`, DROP COLUMN `mail`;
# ALTER TABLE cerlalc_panama.colaboradores DROP COLUMN `documento`, DROP COLUMN `mail`;
# ALTER TABLE cerlalc_paraguay.colaboradores DROP COLUMN `documento`, DROP COLUMN `mail`;
# ALTER TABLE cerlalc_peru.colaboradores DROP COLUMN `documento`, DROP COLUMN `mail`;
# ALTER TABLE cerlalc_republica_dominicana.colaboradores DROP COLUMN `documento`, DROP COLUMN `mail`;
# ALTER TABLE cerlalc_uruguay.colaboradores DROP COLUMN `documento`, DROP COLUMN `mail`;
# DELETE FROM cerlalc_bolivia.log WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_bolivia.log2 WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_chile.log WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_chile.log2 WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_costa_rica.log WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_costa_rica.log2 WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_ecuador.log WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_ecuador.log2 WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_guatemala.log WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_guatemala.log2 WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_honduras.log WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_honduras.log2 WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_mexico.log WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_mexico.log2 WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_panama.log WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_panama.log2 WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_paraguay.log WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_paraguay.log2 WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_peru.log WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_peru.log2 WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_republica_dominicana.log WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_republica_dominicana.log2 WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_uruguay.log WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');
# DELETE FROM cerlalc_uruguay.log2 WHERE tabla = 'colaboradores' AND campo IN ('documento','mail');

# DELETE FROM cerlalc_bolivia.log WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_bolivia.log2 WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_chile.log WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_chile.log2 WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_costa_rica.log WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_costa_rica.log2 WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_ecuador.log WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_ecuador.log2 WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_guatemala.log WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_guatemala.log2 WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_honduras.log WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_honduras.log2 WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_mexico.log WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_mexico.log2 WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_panama.log WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_panama.log2 WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_paraguay.log WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_paraguay.log2 WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_peru.log WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_peru.log2 WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_republica_dominicana.log WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_republica_dominicana.log2 WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_uruguay.log WHERE tabla = 'colaboradores' AND campo IS NULL;
# DELETE FROM cerlalc_uruguay.log2 WHERE tabla = 'colaboradores' AND campo IS NULL;

# mysqldump --opt --skip-comments --databases cerlalc_bolivia cerlalc_chile cerlalc_costa_rica cerlalc_ecuador cerlalc_guatemala cerlalc_honduras cerlalc_mexico cerlalc_panama cerlalc_paraguay cerlalc_peru cerlalc_republica_dominicana cerlalc_uruguay > isbn-cerlalc-2022-11-scrubbed-annas-archive.sql

timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

with open(f"annas_archive_meta__aacid__cerlalc_records__{timestamp}--{timestamp}.jsonl", 'wb') as output_file_handle:
    db = pymysql.connect(host='mariadb', user='root', password='password', database='cerlalc_bolivia', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor, read_timeout=120, write_timeout=120, autocommit=True)

    for db_name in ['cerlalc_bolivia', 'cerlalc_chile', 'cerlalc_costa_rica', 'cerlalc_ecuador', 'cerlalc_guatemala', 'cerlalc_honduras', 'cerlalc_mexico', 'cerlalc_panama', 'cerlalc_paraguay', 'cerlalc_peru', 'cerlalc_republica_dominicana', 'cerlalc_uruguay']:
        print(f"{db_name} ..")
        db.ping(reconnect=True)
        cursor = db.cursor()
        cursor.execute(f'SELECT * FROM {db_name}.titulos')
        for title_row in tqdm.tqdm(list(cursor.fetchall()), bar_format='{l_bar}{bar}{r_bar} {eta}'):
            # cursor.execute('SELECT * FROM excepciones ex WHERE %(id_excepcion)s = ex.id', { 'id_excepcion': title_row['id_excepcion'] })
            # excepciones_rows = list(cursor.fetchall())
            solicitudes_rows = []
            if title_row['id'] is not None:
                cursor.execute('SELECT * FROM solicitudes sol WHERE %(id_solicitud)s = sol.id_solicitud', { 'id_solicitud': title_row['id'] })
                solicitudes_rows = list(cursor.fetchall())
            sellos_rows = []
            if title_row['id_sello'] is not None:
                cursor.execute('SELECT * FROM sellos es WHERE %(id_sello)s = es.id', { 'id_sello': title_row['id_sello'] })
                sellos_rows = list(cursor.fetchall())
            editores_rows = []
            if title_row['id_editor'] is not None:
                cursor.execute('SELECT * FROM editores ed WHERE %(id_editor)s = ed.id', { 'id_editor': title_row['id_editor'] })
                editores_rows = list(cursor.fetchall())
            materias_rows = []
            if title_row['id_materia'] is not None:
                cursor.execute('SELECT * FROM materias mat WHERE %(id_materia)s = mat.clave', { 'id_materia': title_row['id_materia'] })
                materias_rows = list(cursor.fetchall())
            ciudades_rows = []
            if title_row['id_ciudad'] is not None:
                cursor.execute('SELECT * FROM ciudades ciu WHERE %(id_ciudad)s = ciu.id', { 'id_ciudad': title_row['id_ciudad'] })
                ciudades_rows = list(cursor.fetchall())
            descripcionfisica_rows = []
            if title_row['id_descripcionfisica'] is not None:
                cursor.execute('SELECT * FROM descripcionfisica des WHERE %(id_descripcionfisica)s = des.id', { 'id_descripcionfisica': title_row['id_descripcionfisica'] })
                descripcionfisica_rows = list(cursor.fetchall())
            encuadernaciones_rows = []
            if title_row['id_encuadernacion'] is not None:
                cursor.execute('SELECT * FROM encuadernaciones enc WHERE %(id_encuadernacion)s = enc.id', { 'id_encuadernacion': title_row['id_encuadernacion'] })
                encuadernaciones_rows = list(cursor.fetchall())
            presentaciondig_rows = []
            if title_row['id_presentaciondig'] is not None:
                cursor.execute('SELECT * FROM presentaciondig dig WHERE %(id_presentaciondig)s = dig.id', { 'id_presentaciondig': title_row['id_presentaciondig'] })
                presentaciondig_rows = list(cursor.fetchall())
            formatodig_rows = []
            if title_row['id_formatodig'] is not None:
                cursor.execute('SELECT * FROM formatodig fd WHERE %(id_formatodig)s = fd.id', { 'id_formatodig': title_row['id_formatodig'] })
                formatodig_rows = list(cursor.fetchall())
            idiomas_del_rows = []
            if title_row['trad_idioma_del'] is not None:
                cursor.execute('SELECT * FROM idiomas idDel WHERE %(trad_idioma_del)s=idDel.id', { 'trad_idioma_del': title_row['trad_idioma_del'] })
                idiomas_del_rows = list(cursor.fetchall())
            idiomas_al_rows = []
            if title_row['trad_idioma_al'] is not None:
                cursor.execute('SELECT * FROM idiomas idAl WHERE %(trad_idioma_al)s=idAl.id', { 'trad_idioma_al': title_row['trad_idioma_al'] })
                idiomas_al_rows = list(cursor.fetchall())
            idiomas_original_rows = []
            if title_row['trad_idioma_original'] is not None:
                cursor.execute('SELECT * FROM idiomas idOri WHERE %(trad_idioma_original)s=idOri.id', { 'trad_idioma_original': title_row['trad_idioma_original'] })
                idiomas_original_rows = list(cursor.fetchall())
            subtemas_rows = []
            if title_row['id_subtema'] is not None:
                cursor.execute('SELECT * FROM subtemas subt WHERE %(id_subtema)s = subt.id', { 'id_subtema': title_row['id_subtema'] })
                subtemas_rows = list(cursor.fetchall())
            papeles_rows = []
            if title_row['id_papel'] is not None:
                cursor.execute('SELECT * FROM papeles pap WHERE %(id_papel)s = pap.id', { 'id_papel': title_row['id_papel'] })
                papeles_rows = list(cursor.fetchall())
            tintas_rows = []
            if title_row['id_tintas'] is not None:
                cursor.execute('SELECT * FROM tintas tint WHERE %(id_tintas)s = tint.id', { 'id_tintas': title_row['id_tintas'] })
                tintas_rows = list(cursor.fetchall())
            impresiones_rows = []
            if title_row['id_impresion'] is not None:
                cursor.execute('SELECT * FROM impresiones imp WHERE %(id_impresion)s = imp.id', { 'id_impresion': title_row['id_impresion'] })
                impresiones_rows = list(cursor.fetchall())
            gramajes_rows = []
            if title_row['id_gramaje'] is not None:
                cursor.execute('SELECT * FROM gramajes gram WHERE %(id_gramaje)s = gram.id', { 'id_gramaje': title_row['id_gramaje'] })
                gramajes_rows = list(cursor.fetchall())
            departamentos_rows = []
            if title_row['id_departamento'] is not None:
                cursor.execute('SELECT * FROM departamentos dep WHERE %(id_departamento)s = dep.id', { 'id_departamento': title_row['id_departamento'] })
                departamentos_rows = list(cursor.fetchall())
            reimpresiones_rows = []
            if title_row['id'] is not None:
                cursor.execute('SELECT * FROM reimpresiones WHERE %(id_titulo)s = reimpresiones.id_titulo', { 'id_titulo': title_row['id'] })
                reimpresiones_rows = list(cursor.fetchall())
            titulos_autores_rows = []
            if title_row['id'] is not None:
                cursor.execute('SELECT * FROM titulos_autores WHERE %(id_titulo)s = titulos_autores.id_titulo', { 'id_titulo': title_row['id'] })
                titulos_autores_rows = list(cursor.fetchall())
            ibic_rows = []
            if title_row['id_ibic'] is not None:
                cursor.execute('SELECT * FROM ibic WHERE %(id_ibic)s = ibic.id', { 'id_ibic': title_row['id_ibic'] })
                ibic_rows = list(cursor.fetchall())
            audiencia_rows = []
            if title_row['id_audiencia'] is not None:
                cursor.execute('SELECT * FROM audiencia WHERE %(id_audiencia)s = audiencia.id', { 'id_audiencia': title_row['id_audiencia'] })
                audiencia_rows = list(cursor.fetchall())
            tipocontenidodig_rows = []
            if title_row['id_tipocontenidodig'] is not None:
                cursor.execute('SELECT * FROM tipocontenidodig WHERE %(id_tipocontenidodig)s = tipocontenidodig.id', { 'id_tipocontenidodig': title_row['id_tipocontenidodig'] })
                tipocontenidodig_rows = list(cursor.fetchall())
            protecciondig_rows = []
            if title_row['id_protecciondig'] is not None:
                cursor.execute('SELECT * FROM protecciondig WHERE %(id_protecciondig)s = protecciondig.id', { 'id_protecciondig': title_row['id_protecciondig'] })
                protecciondig_rows = list(cursor.fetchall())
            restriccionusodig_rows = []
            if title_row['id_restriccionusodig'] is not None:
                cursor.execute('SELECT * FROM restriccionusodig WHERE %(id_restriccionusodig)s = restriccionusodig.id', { 'id_restriccionusodig': title_row['id_restriccionusodig'] })
                restriccionusodig_rows = list(cursor.fetchall())
            tamanodig_rows = []
            if title_row['id_tamanodig'] is not None:
                cursor.execute('SELECT * FROM tamanodig WHERE %(id_tamanodig)s = tamanodig.id', { 'id_tamanodig': title_row['id_tamanodig'] })
                tamanodig_rows = list(cursor.fetchall())
            tipodescargadig_rows = []
            if title_row['id_tipodescargadig'] is not None:
                cursor.execute('SELECT * FROM tipodescargadig WHERE %(id_tipodescargadig)s = tipodescargadig.id', { 'id_tipodescargadig': title_row['id_tipodescargadig'] })
                tipodescargadig_rows = list(cursor.fetchall())
            permisousodig_rows = []
            if title_row['id_permisousodig'] is not None:
                cursor.execute('SELECT * FROM permisousodig WHERE %(id_permisousodig)s = permisousodig.id', { 'id_permisousodig': title_row['id_permisousodig'] })
                permisousodig_rows = list(cursor.fetchall())
            disponibilidad_rows = []
            if title_row['id_disponibilidad'] is not None:
                cursor.execute('SELECT * FROM disponibilidad WHERE %(id_disponibilidad)s = disponibilidad.id', { 'id_disponibilidad': title_row['id_disponibilidad'] })
                disponibilidad_rows = list(cursor.fetchall())
            estatus_catalogo_rows = []
            if title_row['id_estatus_catalogo'] is not None:
                cursor.execute('SELECT * FROM estatus_catalogo WHERE %(id_estatus_catalogo)s = estatus_catalogo.id', { 'id_estatus_catalogo': title_row['id_estatus_catalogo'] })
                estatus_catalogo_rows = list(cursor.fetchall())
            titulos_idiomas_rows = []
            if title_row['id'] is not None:
                cursor.execute('SELECT * FROM titulos_idiomas WHERE %(id)s = titulos_idiomas.id_titulo', { 'id': title_row['id'] })
                titulos_idiomas_rows = list(cursor.fetchall())
            titulos_thema_rows = []
            if title_row['id'] is not None:
                cursor.execute('SELECT * FROM titulos_thema WHERE %(id)s = titulos_thema.id_titulo', { 'id': title_row['id'] })
                titulos_thema_rows = list(cursor.fetchall())

            for materias_row in materias_rows:
                materias_row['materiact_rows'] = []
                if materias_row['id_categoria'] is not None:
                    cursor.execute('SELECT * FROM materiact matcat WHERE %(id_categoria)s = matcat.id', { 'id_categoria': materias_row['id_categoria'] })
                    materias_row['materiact_rows'] = list(cursor.fetchall())
            for editores_row in editores_rows:
                editores_row['actividades_rows'] = []
                if editores_row['id_actividad'] is not None:
                    cursor.execute('SELECT * FROM actividades act WHERE %(id_actividad)s=act.id', { 'id_actividad': editores_row['id_actividad'] })
                    editores_row['actividades_rows'] = list(cursor.fetchall())
                editores_row['naturalezas_rows'] = []
                if editores_row['id_naturaleza'] is not None:
                    cursor.execute('SELECT * FROM naturalezas nat WHERE %(id_naturaleza)s=nat.id', { 'id_naturaleza': editores_row['id_naturaleza'] })
                    editores_row['naturalezas_rows'] = list(cursor.fetchall())
                editores_row['ciudades_rows'] = []
                if editores_row['id_ciudad'] is not None:
                    cursor.execute('SELECT * FROM ciudades ciu WHERE %(id_ciudad)s = ciu.id', { 'id_ciudad': editores_row['id_ciudad'] })
                    editores_row['ciudades_rows'] = list(cursor.fetchall())
            for titulos_autores_row in titulos_autores_rows:
                titulos_autores_row['colaboradores_rows'] = []
                if titulos_autores_row['id_colaborador'] is not None:
                    cursor.execute('SELECT * FROM colaboradores WHERE %(id_colaborador)s=colaboradores.id', { 'id_colaborador': titulos_autores_row['id_colaborador'] })
                    titulos_autores_row['colaboradores_rows'] = list(cursor.fetchall())
                titulos_autores_row['roles_rows'] = []
                if titulos_autores_row['id_rol'] is not None:
                    cursor.execute('SELECT * FROM roles WHERE %(id_rol)s=roles.id', { 'id_rol': titulos_autores_row['id_rol'] })
                    titulos_autores_row['roles_rows'] = list(cursor.fetchall())
            for titulos_idiomas_row in titulos_idiomas_rows:
                titulos_idiomas_row['idiomas_rows'] = []
                if titulos_idiomas_row['id_idioma'] is not None:
                    cursor.execute('SELECT * FROM idiomas WHERE %(id_idioma)s=idiomas.id', { 'id_idioma': titulos_idiomas_row['id_idioma'] })
                    titulos_idiomas_row['idiomas_rows'] = list(cursor.fetchall())
            for titulos_thema_row in titulos_thema_rows:
                titulos_thema_row['ibic_rows'] = []
                if titulos_thema_row['id_ibic'] is not None:
                    cursor.execute('SELECT * FROM ibic WHERE %(id_ibic)s=ibic.id', { 'id_ibic': titulos_thema_row['id_ibic'] })
                    titulos_thema_row['ibic_rows'] = list(cursor.fetchall())

            uuid = shortuuid.uuid()
            aac_record = {
                "aacid": f"aacid__cerlalc_records__{timestamp}__{uuid}",
                "metadata": {
                    "id": f"{db_name}__titulos__{title_row['id']}",
                    "record": {
                        "titulos": title_row,
                        # "excepciones_rows": excepciones_rows,
                        "solicitudes_rows": solicitudes_rows,
                        "sellos_rows": sellos_rows,
                        "editores_rows": editores_rows,
                        "materias_rows": materias_rows,
                        "ciudades_rows": ciudades_rows,
                        "descripcionfisica_rows": descripcionfisica_rows,
                        "encuadernaciones_rows": encuadernaciones_rows,
                        "presentaciondig_rows": presentaciondig_rows,
                        "formatodig_rows": formatodig_rows,
                        "idiomas_del_rows": idiomas_del_rows,
                        "idiomas_al_rows": idiomas_al_rows,
                        "idiomas_original_rows": idiomas_original_rows,
                        "subtemas_rows": subtemas_rows,
                        "papeles_rows": papeles_rows,
                        "tintas_rows": tintas_rows,
                        "impresiones_rows": impresiones_rows,
                        "gramajes_rows": gramajes_rows,
                        "departamentos_rows": departamentos_rows,
                        "reimpresiones_rows": reimpresiones_rows,
                        "titulos_autores_rows": titulos_autores_rows,
                        "ibic_rows": ibic_rows,
                        "audiencia_rows": audiencia_rows,
                        "tipocontenidodig_rows": tipocontenidodig_rows,
                        "protecciondig_rows": protecciondig_rows,
                        "restriccionusodig_rows": restriccionusodig_rows,
                        "tamanodig_rows": tamanodig_rows,
                        "tipodescargadig_rows": tipodescargadig_rows,
                        "permisousodig_rows": permisousodig_rows,
                        "disponibilidad_rows": disponibilidad_rows,
                        "estatus_catalogo_rows": estatus_catalogo_rows,
                        "titulos_idiomas_rows": titulos_idiomas_rows,
                        "titulos_thema_rows": titulos_thema_rows,
                    },
                },
            }
            output_file_handle.write(orjson.dumps(aac_record, option=orjson.OPT_APPEND_NEWLINE))
            output_file_handle.flush()


