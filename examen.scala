//EXAMEN

// entete = Nom;DateNaissance;VillePays;DateEmbauche;Salaire



//1)

//importation des librairies nécesseaires
import java.time.format.DateTimeFormatter
import java.time.LocalDate

//creer classe salarie
case class Salarie (Nom : String, dateNaiss : LocalDate, villePays : String, dateEmbauche : LocalDate, Salaire : String)


//ouvrir le fichier contenant les informations sur les salariés
var rddSalariesTxt = sc.textFile ("/home/ysr/Desktop/Salaries.csv")
var entete = rddSalariesTxt.first()

//ne pas prendre en compte la premiére ligne
rddSalariesTxt = rddSalariesTxt.filter (e => e != entete)

//parallalize le RDD sur workers
var rddSalariesTxt = sc.parallelize(rddSalariesTxt)

//fonction pour extraire un objet salarie à partir d'une ligne
def creerObjetSalarie (ligneSalarie : String) : Salarie = {
    var nom = ligneSalarie.split (";")(0);
    var dateNaiss = LocalDate.parse(ligneSalarie.split (";")(1), DateTimeFormatter.ofPattern ("dd/MM/yyyy"))
    var villePays = ligneSalarie.split (";")(2);
    var dateEmbauche = LocalDate.parse(ligneSalarie.split (";")(3), DateTimeFormatter.ofPattern ("dd/MM/yyyy"))
    var Salaire = ligneSalarie.split (";")(4);
    return Salarie (nom,dateNaiss,villePays,dateEmbauche,Salaire)
}


//pour chaque ligne du fichier : transformer la ligne en objet salarié
var rddSalariesObj = rddSalariesTxt.map (ligneSalarie => creerObjetSalarie (ligneSalarie))


/*affichage
scala> rddSalariesObj.collect.foreach(println)
Salarie(Gerald,1997-02-20,Paris-FR,2015-06-02,2000 EUR)
Salarie(Bill,2001-07-11,Paris-FR,2016-11-04,3500 USD)
Salarie(Franklin,1994-08-05,Londres-UK,2020-01-28,4000 USD)
Salarie(Alice,1976-06-19,Londres-UK,2021-10-30,2500 EUR)
Salarie(Woodrow,1980-01-16,New York-USA,2019-05-04,4800 EUR)
Salarie(William,1974-10-02,Paris-FR,2000-03-17,5000 EUR)
Salarie(Thomas,2000-07-17,Manchester-UK,2016-12-01,3200 EUR)
Salarie(Benjamin,1990-06-20,Lyon-FR,2019-08-16,1800 EUR)
Salarie(Harry,1996-07-03,New York-USA,2020-10-02,6000 USD)
Salarie(Bob,1980-06-20,Manchester-UK,2000-09-14,2900 USD)
*/

//2)
//extrait à partir de ville-pays la valeur entiere de Pays
def extrairePays (S : String) : String = {
    var pays= S.split("-")(1).toString;
    if (pays == "FR") {return "France";}
    else if (pays == "UK") {return "Royaume Uni";}
    else if (pays == "USA") {return "Etats-Unis";}
    else { return pays;}
    }

//pouch chaque obj Salarie , filtrer le pays , et afficher nom et pays	
var rddSalariesFrancais = rddSalariesObj.filter(S => extrairePays(S.villePays)=="France")
rddSalariesFrancais.map(e=>(e.Nom,extrairePays(e.villePays))).collect.foreach(println)

/*affichage
scala>rddSalariesFrancais.map(e=>(e.Nom,extrairePays(e.villePays))).collect.foreach(println)
(Gerald,France)
(Bill,France)
(William,France)
(Benjamin,France)
*/

//3)

// convertit à partir d'un salaire en USD/EUR , un salaire en EUR
def convertirSalaire (S: String) : Double = {
	var salaire= S.split(" ")(0).toDouble;
	var currency= S.split(" ")(1).toString;
	if (currency =="EUR") { return salaire }
	else {return salaire/1.1 }
	}
	
//Pour chaque obj Salarie, filtrer les salaires, afficher Nom et Salaire	
var rddSalariesUnderpaid= rddSalariesObj.filter(e=> convertirSalaire(e.Salaire)<=3000)
rddSalariesUnderpaid.map(e=>(e.Nom,convertirSalaire(e.Salaire))).collect.foreach(println)

/*affichage
scala> rddSalariesUnderpaid.map(e=>(e.Nom,convertirSalaire(e.Salaire))).collect.foreach(println)
(Gerald,2000.0)
(Alice,2500.0)
(Benjamin,1800.0)
(Bob,2636.363636363636)
*/

//4)
//extrait l'age à la date d'embauche
def getAge (dateNaiss: LocalDate, dateEmbauche: LocalDate) : Int = {
    import java.time.{Period}
    return Period.between(dateNaiss, dateEmbauche).getYears
    }

//filtrer les age < 18
var rddSalariesMinor = rddSalariesObj.filter(e=> getAge(e.dateNaiss, e.dateEmbauche)<18)

//afficher nomn, dateNais, dateEmbauche de lister de mineurs
rddSalariesMinor.map(e=>(e.Nom, e.dateNaiss, e.dateEmbauche)).collect.foreach(println)

/*affichage
scala> rddSalariesMinor.map(e=>(e.Nom, e.dateNaiss, e.dateEmbauche)).collect.foreach(println)
(Bill,2001-07-11,2016-11-04)
(Thomas,2000-07-17,2016-12-01)
*/


//5) 
//calculter somme de salaire et nombre total de salaries
var Salaire= rddSalariesObj.map(e=>(convertirSalaire(e.Salaire),1)).reduce((x,y) => ((x._1+y._1),(x._2+y._2)))

//afficher la moyenne
print(Salaire._1 / Salaire._2)
/*affichage
scala> print(Salaire._1 / Salaire._2)
3420.9090909090905
*/

//6)
//calculer Pays , Somme de salaires / nombre de salaries
var SalaireParPays = rddSalariesObj.map(e => (extrairePays(e.villePays),convertirSalaire(e.Salaire))).reduceByKey ((n1, n2) => (n1+n2)/2)

//afficher la moyenne par pays
SalaireParPays.collect.foreach(println)

/*affichage
scala> SalaireParPays.collect.foreach(println)
(Royaume Uni,2993.181818181818)
(France,2995.4545454545455)
(Etats-Unis,5127.272727272727)
*/





