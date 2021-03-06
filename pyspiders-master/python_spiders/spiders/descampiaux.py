# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import js2xml
import re
from bs4 import BeautifulSoup
import requests
from ..loaders import ListingLoader
from ..items import ListingItem
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date

def getSqureMtr(text):
    list_text = re.findall(r'\d+',text)

    if len(list_text) == 2:
        output = float(list_text[0]+"."+list_text[1])
    elif len(list_text) == 1:
        output = int(list_text[0])
    else:
        output=0

    return int(output)

def getPrice(text):
    list_text = re.findall(r'\d+',text)

    if len(list_text) > 1:
        output = int(list_text[0]+list_text[1])
    elif len(list_text) == 1:
        output = int(list_text[0])
    else:
        output=0

    return int(output)


def cleanText(text):
    text = ''.join(text.split())
    text = re.sub(r'[^a-zA-Z0-9]', ' ', text).strip()
    return text.replace(" ","_").lower()


def num_there(s):
    return any(i.isdigit() for i in s)


def cleanKey(data):
    if isinstance(data,dict):
        dic = {}
        for k,v in data.items():
            dic[cleanText(k)]=cleanKey(v)
        return dic
    else:
        return data


def clean_value(text):
    if text is None:
        text = ""
    if isinstance(text,(int,float)):
        text = str(text.encode('utf-8').decode('ascii', 'ignore'))
    text = str(text.encode('utf-8').decode('ascii', 'ignore'))
    text = text.replace('\t','').replace('\r','').replace('\n','')
    return text.strip()

def clean_key(text):
    if isinstance(text,str):
        text = ''.join([i if ord(i) < 128 else ' ' for i in text])
        text = text.lower()
        text = ''.join([c if 97 <= ord(c) <= 122 or 48 <= ord(c) <= 57 else '_'                                                                                         for c in text ])
        text = re.sub(r'_{1,}', '_', text)
        text = text.strip("_")
        text = text.strip()

        if not text:
            raise Exception("make_key :: Blank Key after Cleaning")

        return text.lower()
    else:
        raise Exception("make_key :: Found invalid type, required str or unicode                                                                                        ")

def traverse( data):
    if isinstance(data, dict):
        n = {}
        for k, v in data.items():
            k = str(k)
            if k.startswith("dflag") or k.startswith("kflag"):
                if k.startswith("dflag_dev") == False:
                    n[k] = v
                    continue

            n[clean_key(clean_value(k))] = traverse(v)

        return n

    elif isinstance(data, list) or isinstance(data, tuple) or isinstance(data, set):                                                                                     
        data = list(data)
        for i, v in enumerate(data):
            data[i] = traverse(v)

        return data
    elif data is None: 
        return ""
    else: 
        data = clean_value(data)
        return data


class QuotesSpider(scrapy.Spider):
    name = "descampiaux_dudicourt_fr_PySpider_france_fr"
    allowed_domains = ['www.descampiaux-dudicourt.fr']
    start_urls = ['www.descampiaux-dudicourt.fr']
    execution_type = 'testing'
    country = 'france'
    locale ='fr'

    def start_requests(self):
        url ='https://www.descampiaux-dudicourt.fr/location.asp'

        yield scrapy.Request(
            url=url, 
            callback=self.parse)

    def parse(self, response):
        soup = BeautifulSoup(response.body)

        max_page = 0
        for page in soup.find("center", class_="pagination").findAll("a"):
            if re.findall('\d+',page.text):
                if int(re.findall('\d+',page.text)[0]) > max_page:
                    max_page = int(re.findall('\d+',page.text)[0])

        for i in range(1,max_page+1):
            sub_url = 'https://www.descampiaux-dudicourt.fr/location.asp?Page={}'.format(i)

            yield scrapy.Request(
                url=sub_url,
                callback=self.get_external_link)

    def get_external_link(self, response):
        soup1 = BeautifulSoup(response.body)

        for el in soup1.findAll("div", class_="resultatintitule"):
            yield scrapy.Request(
                url=el.find("a")['href'], 
                callback=self.get_property_details, 
                meta={'external_link': el.find("a")['href']})

    def get_property_details(self, response):
        item = ListingItem()
        soup2 = BeautifulSoup(response.body)

        external_link = response.meta.get('external_link')
        item["external_link"] = external_link
        
        description = soup2.find("div", id="FicheDescriptifDebutTexte").find("p").text

        zipcode = re.search(r'\d{5}', description)
        if zipcode:
            item['zipcode'] = zipcode.group(0)

        item["description"] = description
        if "terras" in description.lower():
            item["terrace"] = True
        if "zwembad" in description.lower() or "swimming" in description.lower():
            item["swimming_pool"] = True
        if "gemeubileerd" in description.lower()or "am??nag??es" in description.lower() or "furnished" in description.lower():
            item["furnished"]=True

        images = []
        if soup2.find("div", id="PhotoFicheBien2"):
            for img in soup2.find("div", id="PhotoFicheBien2").findAll("img"):
                if "data-src" in str(img):
                    images.append((img['data-src']))
                else:
                    if "/import/0.jpg" not in str(img):
                        images.append((img['src']))
        if images:
            item["images"]= images
            item["external_images_count"]= len(images)
        item["external_id"] = soup2.find("div", id="FicheDescriptifDebutTexte").find("p", class_="fichebienref").text.split(':')[1]
        
        temp_dic = {}
        for dic in soup2.findAll("div", class_="colonnes2"):
            if dic.find("p"):
                temp_dic[dic.find("h2").text] = dic.find("p").text
        temp_dic = cleanKey(temp_dic)

        if "adresse" in temp_dic:
            item["address"] = temp_dic["adresse"]

        for dic in soup2.find("div", class_="fichebienpicto").findAll("div"):

            if "??tage" in dic.text:
                temp_dic["floor"] = dic.text 
            if "Surface" in dic.text:
                temp_dic["surface"] = int(re.findall('\d+',dic.text)[0])
            if "chambre" in dic.text:
                temp_dic["room"] = getSqureMtr(dic.text)
            elif not "chambre" in dic.text:
                if "pi??ce" in dic.text:
                    temp_dic["room"] = getSqureMtr(dic.text)

            if "salle de bain/eau" in dic.text:
                temp_dic["bathroom"] = getSqureMtr(dic.text)
            if "stationnement" in dic.text: 
                temp_dic["parking"] = True

        temp_dic = cleanKey(temp_dic)


        room_count = response.xpath("//div[contains(text(),'pi??ces') or contains(text(),'pi??ce')]/text()").get()
        if room_count:
            item["room_count"] = int(room_count.split()[0].strip())

        if "surface" in temp_dic:
            item["square_meters"] = temp_dic["surface"]

        if "floor" in temp_dic:
            item["floor"]  = temp_dic["floor"]

        if "bathroom" in temp_dic:
            item["bathroom_count"] = temp_dic["bathroom"]
        else:
            bathroom_count = response.xpath("//div[contains(text(),'bain')]/text()").get()
            item["bathroom_count"] = int(bathroom_count.split()[0].strip())

        if "parking" in temp_dic:
            item["parking"] = True

        for dic in soup2.findAll("ul", class_="fichebiendetail"):
            for d in dic.findAll("li"):
                if len(d.text.split(':')) > 1:
                    temp_dic[d.text.split(':')[0]] = d.text.split(':')[1]
        temp_dic = cleanKey(temp_dic)
        

        item["currency"]='EUR'
        rent = getPrice(temp_dic["loyermensuelchargescomprises"].split(",")[0])
        item["rent"] = rent

        if "d_p_tdegarantie" in temp_dic and getSqureMtr(temp_dic["d_p_tdegarantie"]):
            item["deposit"] = getPrice(temp_dic["d_p_tdegarantie"])

        if "dontprovisionmensuelledeschargeslocatives" in temp_dic and getSqureMtr(temp_dic["dontprovisionmensuelledeschargeslocatives"]):
            item["utilities"] = getPrice(temp_dic["dontprovisionmensuelledeschargeslocatives"])

        item["landlord_name"] = soup2.find("div", id="FicheDescriptif").findAll("p")[-1].text.split('\n')[0]

        for tel in soup2.find("div", id="FicheDescriptif").findAll("p")[-1].text.split('\n'):
            if "T??l" in tel:
                item["landlord_phone"] = tel.replace("T??l :", "").strip()

        item["external_source"] = 'Descampiaux_dudicourt_fr_PySpider_france_fr'

        item["city"] = soup2.find("span", class_="ville").text

        property_type = soup2.find("span", class_="intitule").text
        if "tudiant" in property_type.lower() or  "studenten" in property_type.lower() and "appartement" in property_type.lower():
            property_type = "student_apartment"
        elif "appartement" in property_type.lower():
            property_type = "apartment"
        elif "woning" in property_type.lower() or "maison" in property_type.lower() or "huis" in property_type.lower() or "villa" in property_type.lower() or "maison" in property_type.lower() :
            property_type = "house"
        elif "chambre" in property_type.lower() or "kamer" in property_type.lower():
            property_type = "room"
        elif "studio" in property_type.lower():
            property_type = "studio"
        else:
            property_type = "NA"
        item["property_type"] = property_type

        item["title"] = soup2.find("span", class_="intitule").text + ' - ' + soup2.find("span", class_="ville").text

        lat_lng = response.xpath("//script[contains(.,'mapCentre')]/text()").re_first(r"mapCentre=\[(\d+.\d+, -*\d+.\d+)\]")
        if lat_lng:
            item['latitude'] = lat_lng.split(',')[0].strip()
            item["longitude"] = lat_lng.split(',')[1].strip()
        
        if property_type in ["apartment", "house", "room", "property_for_sale", "student_apartment", "studio"]:
            yield item