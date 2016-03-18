#include <iostream>
#include <string>
#include <fstream>
using namespace std;

string filename[] = {
  "pg-being_ernest.txt",
  "pg-dorian_gray.txt",
  "pg-dracula.txt",
  "pg-emma.txt",
  "pg-frankenstein.txt",
  "pg-great_expectations.txt",
  "pg-grimm.txt",
  "pg-huckleberry_finn.txt",
  "pg-les_miserables.txt",
  "pg-metamorphosis.txt",
  "pg-moby_dick.txt",
  "pg-sherlock_holmes.txt",
  "pg-tale_of_two_cities.txt",
  "pg-tom_sawyer.txt",
  "pg-ulysses.txt",
  "pg-war_and_peace.txt"
};
int main() {
  int count = 0;
  for (int i = 0; i < 16; ++i) {
    ifstream fin(filename[i].c_str());
    string tmp;
    while (fin >> tmp) {
      if (tmp == "that") ++count;
    }
  }
  cout << count << endl;
  return 0;
}
