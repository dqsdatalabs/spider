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
from scrapy import Request,FormRequest
# import geopy
# from geopy.geocoders import Nominatim
# from geopy.extra.rate_limiter import RateLimiter

# locator = Nominatim(user_agent="myGeocoder")

# def getAddress(lat,lng):
#     coordinates = str(lat)+","+str(lng) # "52","76"
#     location = locator.reverse(coordinates)
#     return location

def extract_city_zipcode(_address):
    zip_city = _address.split(", ")[1]
    zipcode, city = zip_city.split(" ")
    return zipcode, city

def getSqureMtr(text):
    list_text = re.findall(r'\d+',text)

    if len(list_text) == 2:
        output = float(list_text[0]+"."+list_text[1])
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


class QuotesSpider1(scrapy.Spider):
    name = "remiserais_immobilier_fr_PySpider_france_fr"
    allowed_domains = ['www.remiserais-immobilier.fr']
    start_urls = ['www.remiserais-immobilier.fr']
    execution_type = 'testing'
    country = 'france'
    locale ='fr'

    def start_requests(self):
        
        start_urls = [
            {
                "url": "http://www.remiserais-immobilier.fr/catalog/advanced_search_result.php?action=update_search&search_id=1696995519551326&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=",
                "property_type": "house"
            },
            {
                "url": "http://www.remiserais-immobilier.fr/catalog/advanced_search_result.php?action=update_search&search_id=1696995519551326&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=",
                "property_type": "apartment"
            }
        ]
        
        for item in start_urls:
            yield scrapy.Request(
                item.get('url'),
                callback=self.get_external_link,
                meta={"property_type": item.get('property_type')}
            )

    def get_external_link(self, response):
        soup1 = BeautifulSoup(response.body)
        
        for el in soup1.find("div", id="listing_bien").findAll("div", class_="link-product"):
            yield scrapy.Request(
                url=el.find("div").find("a")['href'].replace('..', 'http://www.remiserais-immobilier.fr'), 
                callback=self.get_property_details, 
                meta={'external_link': el.find("div").find("a")['href'].replace('..', 'http://www.remiserais-immobilier.fr'), 'property_type': response.meta.get('property_type')})

        next_page = response.xpath("//a[contains(.,'Suivante')]/@href").get()
        if next_page:
            yield scrapy.Request(response.urljoin(next_page), callback=self.get_external_link, meta={'property_type': response.meta.get('property_type')})
        
    def get_property_details(self, response):
        item = ListingItem()
        soup2 = BeautifulSoup(response.body)
        if "http://www.remiserais-immobilier.fr/ville_bien/" in response.url:
            return
        # external_link = response.meta.get('external_link')
        # item["external_link"] = response.xpath("substring-after(//p[@class='ref-listing']/text(),': ')").extract_first().strip()
        # item["external_link"] = external_link
        item["external_link"] = response.url
        
        title = soup2.find("div", id="content_intro_header")
        if title:
            item["title"] = title.text.strip()
        
        item["landlord_name"] = "L'immobilier par REMI SERAIS - Argentan"
        item["landlord_phone"] = "02.33.36.78.78"
        description = "".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
        if description:
            item["description"] = description.strip()
        if "garage" in description.lower() or "parking" in description.lower():
            item["parking"] = True
        if "terras" in description.lower():
            item["terrace"] = True
        if "zwembad" in description.lower() or "swimming" in description.lower():
            item["swimming_pool"] = True
        if "garage" in description.lower() or "parking" in description.lower():
            item["parking"] = True 

        images = [response.urljoin(x) for x in response.xpath("//ul[@class='slides']/li/a/@href").getall()]
        if images:
            item["images"] = images

        temp_dic = {}
        # for li in soup2.find("div", id="content_details").findAll("li"):
        #     temp_dic[li.text.split(':')[0]] = li.text.split(':')[1]

        if soup2.find("div", class_="General"):
            for li in soup2.find("div", class_="General").findAll("li"):
                for l in li.findAll("div", class_="row"):
                    temp_dic[l.findAll("div")[0].text] = l.findAll("div")[1].text.strip()

        if soup2.find("div", class_="localisation"):
            for li in soup2.find("div", class_="localisation").findAll("li"):
                for l in li.findAll("div", class_="row"):
                    temp_dic[l.findAll("div")[0].text] = l.findAll("div")[1].text.strip()

        if soup2.find("div", class_="aspects_financiers"):
            for li in soup2.find("div", class_="aspects_financiers").findAll("li"):
                for l in li.findAll("div", class_="row"):
                    temp_dic[l.findAll("div")[0].text] = l.findAll("div")[1].text.strip()

        if soup2.find("div", class_="interieur"):
            for li in soup2.find("div", class_="interieur").findAll("li"):
                for l in li.findAll("div", class_="row"):
                    temp_dic[l.findAll("div")[0].text] = l.findAll("div")[1].text.strip()

        if soup2.find("div", class_="surfaces"):
            for li in soup2.find("div", class_="surfaces").findAll("li"):
                for l in li.findAll("div", class_="row"):
                    temp_dic[l.findAll("div")[0].text] = l.findAll("div")[1].text.strip()

        if soup2.find("div", class_="autres"):
            for li in soup2.find("div", class_="autres").findAll("li"):
                for l in li.findAll("div", class_="row"):
                    temp_dic[l.findAll("div")[0].text] = l.findAll("div")[1].text.strip()

        if soup2.find("div", class_="diagnostics"):
            for li in soup2.find("div", class_="diagnostics").findAll("li"):
                for l in li.findAll("div", class_="row"):
                    temp_dic[l.findAll("div")[0].text] = l.findAll("div")[1].text.strip()

        temp_dic = cleanKey(temp_dic)
        # print(temp_dic)
        # print ("\n")
        # item["external_id"] = temp_dic["r_f_rence"]
        external_id = response.xpath("//div[@class='col-sm-12']/ul/li/span[contains(.,'Référence')]/text()").get()
        if external_id:
            item["external_id"] = external_id.split(":")[1].strip()

        item["property_type"] = response.meta.get('property_type')
 
        room = response.xpath("//div[@class='row']/div[.='Chambres']/following-sibling::div/b/text()").extract_first()
        if room:
            item["room_count"] = int(room.strip())
        elif not room:
            room1=response.xpath("//div[@class='row']/div[.='Nombre pièces']/following-sibling::div/b/text()").extract_first()
            if room1:
                item["room_count"]=int(room1.strip())
        
         

        if "salle_s_debains" in temp_dic:
            item["bathroom_count"] = int(temp_dic["salle_s_debains"])            

        if "ville" in temp_dic:
            item["city"] = temp_dic["ville"].strip()

        if "surfacehabitable" in temp_dic:
            item["square_meters"] = getSqureMtr(temp_dic["surfacehabitable"])
        else:
            meters = response.xpath("//li[@class='list-group-item odd']/div/div[.='Surface']/following-sibling::div/b/text()").extract_first()
            if meters:
               s_meters = meters.replace("m2","").strip()
               item["square_meters"] = int(float(s_meters))
        
        if "d_p_tdegarantie" in temp_dic:
            item["deposit"]  = getSqureMtr(temp_dic["d_p_tdegarantie"])

        if "provisionsurcharges" in temp_dic:
            item["utilities"] = getSqureMtr(temp_dic["provisionsurcharges"])

        if "valeurconsoannuelle_nergie" in temp_dic:
            item["energy_label"] = temp_dic["valeurconsoannuelle_nergie"]

        if "etage" in temp_dic:
            item["floor"] = temp_dic["etage"].strip()

        item["currency"]='EUR'

        if "codepostal" in temp_dic:
            item["zipcode"] = temp_dic["codepostal"]

        item["external_source"] = 'remiserais_immobilier_fr_PySpider_france_fr'

        if "meubl" in temp_dic:
            if temp_dic["meubl"] == "Oui":
                item["furnished"] = True
            if temp_dic["meubl"] == "Non":
                item["furnished"] = False

        if "ascenseur" in temp_dic:
            if temp_dic["ascenseur"] == "Oui":
                item["elevator"] = True
            if temp_dic["ascenseur"] == "Non":
                item["elevator"] = False

        if "piscine" in temp_dic:
            if temp_dic["piscine"] == "Oui":
                item["swimming_pool"] = True
            if temp_dic["piscine"] == "Non":
                item["swimming_pool"] = False

        address = response.xpath("//span[@class='alur_location_ville']/text()").get()
        if address:
            item["address"] = address
        rent = response.xpath("//div[@id='content_details']//span[@class='alur_loyer_price']/text()").get()
        if rent:
            item["rent"]  = getSqureMtr(temp_dic["loyermensuelhc"])



        yield item



