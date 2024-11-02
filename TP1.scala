LAUNCH SCALA DUMB FUCK

MASTER
./bin/spark-class org.apache.spark.deploy.master.Master

WORKER
./bin/spark-class org.apache.spark.deploy.worker.Worker spark://192.168.0.25:7077 -m 1G -c 2

SHELL
./bin/spark-shell --master spark://192.168.0.25:7077 --name Name



Manip 10:
var nbs = (0 to 10)toList

Manip 11:
var rddNbs = sc.parallelize(nbs)

manip 12:
rddNbs.collect.foreach(println)
rddNbs.collect.foreach( e => print( e + " * "))

Manip 14:
rddNbs.getNumPartitions

manip 15:
rddNbs.glom.collect


manip 17:
rdd.partitioner



manip 18: 
rdd.count

manip 19:
rdd.collect.take(3)

manip 20:
rdd.filter(x=>x%2==0).collect.()

manip 21:reduce((e1,e2) => e1+e2)
rdd.collect.sum
rdd.collect.

manip 22: 
rdd.filter(x=>x%2==0).collect.sum

manip 23:

1ere methode
_______________________________________
def parite (e : Int ) : String = {
	if ( e % 2 ==0) return "pair";
	else return "impair" ;
	}
	
var rddParite = rdd.map(e =>(parite(e),e))


def additionner ( a : Int  , b : Int ) : Int = return a+b;

var rddPariteSomme = rddParite.reduceByKey((nb1,nb2) => nb1+nb2))

rddPariteSomme.collect()
__________________________________________
2eme methode

rdd.map(e=>(if (e%2==0) "pair" else "impair",e)).reduceByKey((nb1,nb2)=> 2).collect()


manip 25:

var villes = List("Paris FR 5", "Stuttgart DE 0.9", "Lyon FR 2", "Londres UK 8", "Berlin DE 4", "Marseille FR 3", "Liverpool UK 1.5", "Munich DE 1")

var villesRDD = sc.parallelize (villes)

villesRDD.collect.foreach(println)

Manip 27 :
def extractVille (chaineVille: String) : String = {
     | var elts = chaineVille.split (" ")
     | var ville = elts(0)
     | return ville
     | }
 
var nomVillesRDD = villesRDD.map (villeStr => extractVille(villeStr))
nomVillesRDD.take (10)


 def extractInfo (chaineVille: String, info : String) : String = {
     | var indice = 0;
     | if (info == "pays") indice = 1;
     | if (info == "population") indice = 2;
     | return chaineVille.split (" ")(indice);
     | }

Manip 31 :
var popRDD = villesRDD.map (villeStr => extractInfo(villeStr, "population").toDouble)
popRDD.sum
ou
popRDD.reduce ((e1, e2) => e1+e2) 

Mais 32 :
var rddKVPaysPop = villesRDD.map (villeStr => (extractInfo(villeStr, "pays"), extractInfo(villeStr, "population").toDouble))
rddKVPaysPop.reduceByKey ((v1, v2) => v1+v2)
rddPopTotByPays.collect

Manip 34:
 fileRDD.count()
 
Manip 35:
    var mots = fileRDD.filter (ligne => ligne.contains ("dessin"))
    mots.collect.foreach(println)
    
Manip 36 :
var fileRDDSansVide = fileRDD.filter (ligne => ! ligne.isEmpty)
var fTab = fileRDDSansVide.map (ligne =>ligne.split (" "))
fNbMostsParligne.sum

2ème solution
var motsRDD = fileRDDSansVide.flatMap (l => l.split(" "))
motsRDD.count

Manip 37 :
var nbMots = rddTxt.map (ligne => ligne.length).sum
nbMots.take (5)

Manip 40 :
var occMots = rddTxt.filter (ligne => !ligne.isEmpty).flatMap (ligne => ligne.split (" ")).map (mot => (mot, 1)).reduceByKey ((nb1, nb2) => nb1+nb2)
occMots.take (20)

Manip 41 :
import java.time.LocalDate
case class Personne (numero : Int, nom : String, prenom : String, dateNaiss : LocalDate, ville : String)
var rddPersonnelsTxt = sc.textFile ("/Volumes/developpement/data/2021-2022/m1-p8-spark-mars-2022/Personnel.csv")
var entete = rddPersonnelsTxt.first()
rddPersonnelsTxt = rddPersonnelsTxt.filter (e => e != entete)

import java.time.LocalDate
case class Personne (numero : Int, nom : String, prenom : String, dateNaiss : LocalDate, ville : String, logement : String)
var rddPersonnelsTxt = sc.textFile ("/Volumes/developpement/data/2021-2022/m1-p8-spark-mars-2022/Personnel.csv")
var entete = rddPersonnelsTxt.first()
rddPersonnelsTxt = rddPersonnelsTxt.filter (e => e != entete)

import java.time.format.DateTimeFormatter
def creerObjetPersonnel (lignePersonnel : String) : Personnel = {
    var num = lignePersonnel.split (";")(0).toInt;
    var nom = lignePersonnel.split (";")(1);
    var prenom = lignePersonnel.split (";")(2);
    var dateNaiss = LocalDate.parse(lignePersonnel.split (";")(3), DateTimeFormatter.ofPattern ("dd/MM/yyyy"))
    var adresse = lignePersonnel.split (";")(4);
    var logement = lignePersonnel.split (";")(5);
    return Personnel (num, nom, prenom, dateNaiss, adresse, logement)
}
var rddPersonnelsObj = rddPersonnelsTxt.map (lignePersonnel => creerObjetPersonnel (lignePersonnel))

Manip 46 :
var rdd2000Paris = rddPersonnelsObj.filter (p => p.dateNaiss.getYear() <= 2000 && p.ville=="Paris")

Manip 46 :
var rdd2000Paris = rddPersonnelsObj.filter (p => p.dateNaiss.getYear() <= 2000 && p.ville=="Paris")

Manip 47 :
rddPersonnelsObj. map (p => (p.ville, 1)).reduceByKey ((n1, n2) => n1+n2)

Manip 50 :
case class Employeur (numero : Int, employeur : String, salaire : Double)

var rddEmployeursTxt = sc.textFile ("/Volumes/developpement/data/2021-2022/m1-p8-spark-mars-2022/Employeur.csv")
var entete = rddEmployeursTxt.first()
rddEmployeursTxt = rddEmployeursTxt.filter (e => e != entete)

def creerObjetEmployeur (ligneEmp : String) : Employeur = {
    var num = ligneEmp.split (";")(0).toInt;
    var emp = ligneEmp.split (";")(1);
    var sal = ligneEmp.split (";")(2).toDouble;
    return Employeur (num, emp, sal)
}
var rddEmloyeursObj = rddEmployeursTxt.map (ligneEmp => creerObjetEmployeur (ligneEmp))
rddEmloyeursObj.collect

var rddPersonnelsObjJoin = rddPersonnelsObj.map (p => (p.numero, p))
var rddEmloyeursObjJoin = rddEmloyeursObj.map (e => (e.numero, e))

var rddPersEmpJoin = rddPersonnelsObjJoin.join (rddEmloyeursObjJoin)

var salaireMinParVille = rddPersEmpJoin.map (e=> (e._2._1.ville, e._2._2.salaire) ).reduceByKey ((s1, s2)=> Math.min(s1,s2))
salaireMinParVille.collect

