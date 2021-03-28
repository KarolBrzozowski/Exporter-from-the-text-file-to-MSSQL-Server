using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Windows;


namespace ConnectZbk
{
    class Program
    {
        static void Main(string[] args)
        {
            // Deklaracja zmiennych 
            string server = "255.255.255.255", database = "databaseName", udi = "login", pwd = "pasword";
            Int32 zm = 0;
            string wiersz = "", ttt;
            string name = "krowy", data = "20170524";

            char[] uzk = new char[800];
            int k = 0, j = 0, size, licz = 0;
            krowy[] tab1 = new krowy[24];
   
            string tabela = "CREATE TABLE " + name + "(nr_krowy VARCHAR(14) PRIMARY KEY,", znak, buff;
            DataTable myTable = new DataTable();


            AddInfo(ref server, ref database, ref udi, ref pwd, ref name, ref data);
            SqlConnection conn = new SqlConnection("Server=" + server + ";Database=" + database + ";Uid=" + udi + ";Pwd=" + pwd);
            conn.Open();
            
            //SqlCommand cmd = new SqlCommand("SELECT * FROM karol", conn);
            SqlCommand cmd1 = new SqlCommand();

            FileStream fs = new FileStream("C:\\moje\\dozbk.txt", FileMode.Open, FileAccess.Read);



            StreamReader sr = new StreamReader(fs, Encoding.UTF8);
            //============== Wczytanie pliku dobuh.txt który przechowuję strukturę pliku krowy.txt =================================================

            while (sr.Peek() != -1)
            {


                ttt = GetNextWord(sr); // czytanie pliku słowo po słowie.


                AddFileToMatrix(ref tab1, k, ttt, sr);      // dodawanie danych do tab2
                if(k>0)
                    if (k < 23)
                    {
                        if (tab1[k].rodzaj[0] == 'I')
                            tabela += tab1[k].nazwa + " " + tab1[k].rodzaj + ",";
                        else
                            tabela += tab1[k].nazwa + " " + tab1[k].rodzaj + "(" + Convert.ToString(tab1[k].liczba_znakow) + "),";
                    }
                    else  // Dla poprawnego zakończenia zapytanie SQL
                    {
                        if (tab1[k].rodzaj[0] == 'I')
                            tabela += tab1[k].nazwa + " " + tab1[k].rodzaj + ");";
                        else
                            tabela += tab1[k].nazwa + " " + tab1[k].rodzaj + "(" + Convert.ToString(tab1[k].liczba_znakow) + "));";

                    }
                

                k++;
                //if (k == 2000) break;
            }
            //================================================================================================================================


            myTable = CreateDataTable(tab1, "krowy"); // tworzenie tablicy przechowującej zgodnej z tabelą mssql dane na podstawie tab1 

            //==============Program pyta czy utworzyć nowa tabelę jeśli nie dane zostaną dopisane do wybranej istniejącej.===========================================================


            Console.WriteLine("Jesli chcesz utworzc nowa tabela w bazie danych nacisnij t, jesli nie n");
            znak = Convert.ToString(Console.ReadLine());
            Console.WriteLine(tabela);
            if (znak[0] == 't')
            {

                cmd1 = new SqlCommand(tabela, conn);
                cmd1.ExecuteNonQuery();
            }


            //Console.WriteLine(tabela);
            //=================================================================================================================================

            Console.WriteLine(DateTime.Now.ToString());
            //znak = Convert.ToString(Console.ReadLine());

            fs.Close();
            fs = new FileStream("C:\\moje\\zbkrod.txt", FileMode.Open, FileAccess.Read);
            sr = new StreamReader(fs, Encoding.Default, true);
            k = 0;

            //===================== Czytanie pliku krowy.txt wiersz po wierszu ================================================================
            while (!sr.EndOfStream)
            {
                wiersz = sr.ReadLine();
                size = wiersz.Length;


                if (size > 0) // Jeśli wiersz nie jest pusty.
                {

                    buff = "";
                    
                    if (k > 0)            // dla ilości wierszów > od zera, program działa z opuźnieniem o jeden wiersz
                    {                  // dzięki temu jest w stanie sprawdzić czy dany osobnik składa się z dwóch czy jednego wiersza.

                        SprNr(ref tab1, data);            // sprawdzanie numeru
                        AddRowToDataTable(ref myTable, tab1); // dodawanie wiersza do tabeli typu DataTable
                        zerowanie(ref tab1);
                            
                    }

                    for (int i = 0; i < 21; i++) // W pierwszym wierszu osobnika może być 21 kolumn
                    {
                        if (tab1[i].od_znaku < size)
                        {
                            // Kopiowanie po kawałku( poszczególnych kolumn) i zapisanie je w tab1.
                            CopPiece(ref tab1[i].dane, ref wiersz, tab1[i].od_znaku, tab1[i].do_znaku, size);

                            if (tab1[i].rodzaj[0] == 'I') // Jeśli dane są typu integer są sprawdzane
                            {
                                SprInt(ref tab1[i].dane);
                            }
                        }
                        else break;
                    }


                    if (licz % 200000 == 0 && licz > 1)  // Wysyłanie danych do bazy porcjami po 200000 osobników  
                    {
                        Console.WriteLine("Wczytywanie danych do bazy danych...");
                        BulkCopyToData(ref myTable, conn, name);
                        myTable = new DataTable();
                        myTable = CreateDataTable(tab1, name);


                    }
                    licz++; // licznik poszczególnych osobników.

                    
                   

                }

                k++;
                if (k % 10000 == 0) // licznik pojedyńczych wierszy w pliku krowy.txt
                    Console.WriteLine(k + " Wierszy z pliku wczytano...");

            }


            // Program działa z opuźnieniem jednego wiersza dlatego jest potrzeba osobnego wysłania do bazy ostatniego wiersza.
            AddRowToDataTable(ref myTable, tab1);
            Console.WriteLine("Wczytywanie danych do bazy danych...");
            BulkCopyToData(ref myTable, conn, name);

            Console.WriteLine(DateTime.Now.ToString() + " ilosc =" + licz);

            // Zamknięcie zmiennych plikowych i połączenia z bazą mssql
            fs.Close();
            conn.Close();


        }
        //********************************************************************************************************************************

        //********************************************************************************************************************************
        static void AddInfo(ref string server, ref string database, ref string udi, ref string pwd, ref string name, ref string data)
        {
            char z;
            Console.WriteLine("***************Program importujący dane z pliku zbkrod.txt do bazy danych MS Sql*********************");
            Console.WriteLine("Data dodania '" + data + "' czy dokonać zmiany t/n");
            z = Convert.ToChar(Console.ReadLine());
            if (z == 't')
                data = Convert.ToString(Console.ReadLine());

            Console.WriteLine("Ip servera: '" + server + "' czy dokonać zmiany t/n");
            z = Convert.ToChar(Console.ReadLine());
            if (z == 't')
                server = Convert.ToString(Console.ReadLine());

            Console.WriteLine("Nazwa bazy: '" + database + "' czy dokonać zmiany t/n");
            z = Convert.ToChar(Console.ReadLine());
            if (z == 't')
                database = Convert.ToString(Console.ReadLine());

            Console.WriteLine("Login do bazy: '" + udi + "' czy dokonać zmiany t/n");
            z = Convert.ToChar(Console.ReadLine());
            if (z == 't')
                udi = Convert.ToString(Console.ReadLine());

            Console.WriteLine("Haslo to domyślnie *******  czy dokonać zmiany t/n");
            z = Convert.ToChar(Console.ReadLine());
            if (z == 't')
                pwd = Convert.ToString(Console.ReadLine());

            Console.WriteLine("Nazwa tabeli '" + name + "' czy dokonać zmiany t/n");
            z = Convert.ToChar(Console.ReadLine());
            if (z == 't')
                name = Convert.ToString(Console.ReadLine());

        }
        //=============================================================================================================================================
        static void BulkCopyToData(ref DataTable myTable, SqlConnection conn, string name)
        {
            using (SqlTransaction myTran = conn.BeginTransaction())
            {


                using (SqlBulkCopy Bulki = new SqlBulkCopy(conn, SqlBulkCopyOptions.KeepIdentity, myTran))
                {
                    Bulki.BatchSize = 100000;
                    Bulki.BulkCopyTimeout = 100000;
                    Bulki.DestinationTableName = name;


                    try
                    {
                        Bulki.WriteToServer(myTable);
                        myTran.Commit();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        myTran.Rollback();
                    }

                }
            }
        }
        //============================================================================================================================================
        static void AddRowToDataTable(ref DataTable myTable, krowy[] tab1)
        {
            Int32 dl = 0;
            DataRow myRow = myTable.NewRow();
            for (int i = 0; i < 24; i++)
            {
                dl = (tab1[i].dane).Length;
                if (dl > 0)
                {
                    if (tab1[i].nazwa[0] == 'd' && tab1[i].nazwa[1] == 'a' && dl == 4)
                        AddZeroToDat(ref tab1[i].dane);

                    myRow[tab1[i].nazwa] = (tab1[i].dane).ToString();

                }
            }

            myTable.Rows.Add(myRow);
        }

        //=============================================================================================================================================
        static void AddZeroToDat(ref string tab)
        {
            tab += "0000";
        }

        static void GNextWord(ref string sr)
        {
            String word = "";
            int dl = sr.Length;

            for (int i = 0; i < dl; i++)
            {

                if (sr[i] == ' ' || sr[i] == '\t' || sr[i] == '\n' || sr[i] == '\r')
                {

                    break;
                }
                else
                    word += sr[i];
            }
            sr = word;
        }

        //============================================================================================================================================
        static String GetNextWord(StreamReader sr)
        {
            String word = "";
            char c;
            while (sr.Peek() >= 0)
            {
                c = (char)sr.Read();
                if (c.Equals(' ') || c.Equals('\t') || c.Equals('\n') || c.Equals('\r'))
                {
                    break;
                }
                else
                    word += c;
            }
            return word;
        }
        //=============================================================================================================================================
        static void CopPiece(ref string zm1, ref string lan, Int32 od, Int32 doo, int siz)
        {
            zm1 = "";
            int i, z = 1;
            for (i = od; i <= doo; i++)
            {
                if (doo - od == 0 && lan[od] == ' ') break;
                if (i == siz) break;
                if (i == doo) z = 0;
                if (lan[i] == ' ' && lan[i + z] == ' ')
                {

                }
                else
                {
                    if (lan[i] == 39)
                        zm1 += '`';
                    else
                        zm1 += lan[i];
                }
            }
            zm1 = zm1.Trim();
        }

        //============================================================================================================================================
        static void SprInt(ref string t)
        {

            string zm = "";
            char znak;
            t = t.Trim();
            int siz = t.Length;
            if (siz > 0)
            {
                for (int i = 0; i < siz; i++)
                {
                    znak = t[i];
                    //if (znak == ' ') znak= '0';
                    if (znak < 48 || znak > 57)
                    {
                        Console.WriteLine("niewlasciwy int " + t + "   " + znak);
                        //t = "";
                        //break;
                    }
                    else
                        zm += znak;
                }
                t = zm;
            }
            else
                t = "";
        }
        //==============================================================================================================================================
        static void zerowanie(ref krowy[] tab)
        {
            for (int i = 0; i < 24; i++)
            {
                tab[i].dane = "";
            }
        }
        //=============================================================================================================================================
        static void AddFileToMatrix(ref krowy[] tab, int i, string slowo, StreamReader srr)
        {
            if (slowo == "i") slowo = "INT";
            else slowo = "VARCHAR";
            tab[i].rodzaj = slowo;

            slowo = GetNextWord(srr);
            tab[i].liczba_znakow = Convert.ToInt32(slowo);

            slowo = GetNextWord(srr);
            tab[i].od_znaku = Convert.ToInt32(slowo) - 1;

            slowo = GetNextWord(srr);
            tab[i].do_znaku = Convert.ToInt32(slowo) - 1;

            slowo = GetNextWord(srr);
            tab[i].nazwa = slowo;
            tab[i].dane = "";
        }
        //=============================================================================================================================================
        static DataTable CreateDataTable(krowy[] listaKolumn1, string tablename)
        {
            string zm = "";
            string query = "select top 1 *from " + tablename;
            DataTable tab = new DataTable();
            for (int i = 0; i < 24; i++)
            {
                if (listaKolumn1[i].rodzaj[0] == 'V')
                    tab.Columns.Add(new DataColumn(listaKolumn1[i].nazwa, typeof(string)));
                else
                    tab.Columns.Add(new DataColumn(listaKolumn1[i].nazwa, typeof(int)));

            }

            
            return tab;
        }
        //=============================================================================================================================================
        static void SprNr(ref krowy[] tab1, string data)
        {

            int dl = (tab1[1].dane).Length;
            tab1[21].dane = "";
            tab1[21].dane = "";
            tab1[23].dane = data;
            if (dl == 14)
            {
                tab1[21].dane = "1";

            }
            else
            {
                if ((tab1[1].dane[0] > 64 && tab1[1].dane[0] < 91) && (tab1[1].dane[1] > 64 && tab1[1].dane[1] < 91) && (dl > 2))
                {

                    string nap = "";
                    nap += tab1[1].dane[0];
                    nap += tab1[1].dane[1];

                    for (int i = 0; i < 14 - dl; i++)
                    {
                        nap += '0';
                    }
                    for (int i = 2; i < dl; i++)
                    {
                        if (tab1[1].dane[i] > 47 && tab1[1].dane[i] < 58)
                            nap += tab1[1].dane[i];
                        else
                        {
                            nap = "";
                            break;
                        }
                    }
                    tab1[22].dane = nap;
                }
                tab1[21].dane = "0";

            }

        }
        //====================================================================================================================================
        struct krowy
        {
            public
            Int32 liczba_znakow, od_znaku, do_znaku;
            public
            string nazwa, rodzaj, dane;

        };
    }
}
//=============================================================================================================================================
    

