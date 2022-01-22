# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
from datetime import datetime
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date
import re,json
from bs4 import BeautifulSoup

def getSqureMtr(text):
    list_text = re.findall(r'\d+',text)

    if len(list_text) == 3:
        output = float(list_text[0]+"."+list_text[1])
    elif len(list_text) == 2:
        output = float(list_text[0]+"."+list_text[1])
    elif len(list_text) == 1:
        output = int(list_text[0])
    else:
        output=0

    return int(output)

def getPrice(text):
    list_text = re.findall(r'\d+',text)

    if len(list_text) == 2:
        output = float(list_text[0]+list_text[1])
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

class MySpider(Spider):
    name = 'regiesaintlouis_fr'
    allowed_domains = ['www.regiesaintlouis.com']
    start_urls = ['www.regiesaintlouis.com']
    execution_type = 'testing'
    country = 'france'
    locale ='fr'

    data={
        "nature": "2",
        "type[]": "",
        "price": "",
        "reference": "",
        "age": "",
        "tenant_min": "",
        "tenant_max": "",
        "rent_type": "",
        "currency": "EUR"
    }
    def start_requests(self):
        url="http://www.regiesaintlouis.com/fr/recherche/"

        property_value = ["1","2","6"]
        for p_t in property_value:
            
            self.data["type[]"] = p_t
            if "1" ==  p_t or "6" == p_t: 
                property_type = "apartment"
            elif "2" == p_t:
                property_type = "house"

            yield FormRequest(
                url=url,
                formdata = self.data,
                callback=self.parse,
                meta = {"property_type":property_type, "type": p_t}
                )

    def parse(self, response, **kwargs):
        soup = BeautifulSoup(response.body,"html.parser")

        property_type = response.meta["property_type"]
        p_type = response.meta["type"]
        if soup.find("ul", class_="pager"):
            all_pages = soup.find("ul", class_="pager").find_all("li")
            if num_there(all_pages[-1].text):
                count = int(all_pages[-1].text)
            elif num_there(all_pages[-2].text):
                count = int(all_pages[-2].text)
            elif num_there(all_pages[-3].text):
                count = int(all_pages[-3].text)

            for c in range(count+1):
                url = "http://www.regiesaintlouis.com/fr/recherche/"+str(c+1)
                self.data["type[]"] = p_type
                yield FormRequest(
                    url=url,
                    callback=self.get_page_details,
                    formdata = self.data,
                    meta = {"property_type":property_type}
                    )

    def get_page_details(self, response, **kwargs):
        soup = BeautifulSoup(response.body,"html.parser")

        property_type = response.meta["property_type"]
        articles = soup.find("ul",class_="ads").find_all("li")
        for ech_art in articles:
            external_link = "http://www.regiesaintlouis.com"+ech_art.find("a")["href"]
            yield Request(
                url=external_link,
                callback=self.get_property_details,
                meta = {"property_type":property_type}
                )

    def get_property_details(self, response, **kwargs):
        item_loader = ListingLoader(response=response)
        soup = BeautifulSoup(response.body,"html.parser")
        str_soup = str(soup)

        match = re.findall("= L.marker(.+);",str_soup)
        if match:
            lat_lng = eval(match[0].split(",{")[0].replace("(",""))
            latitude = str(lat_lng[0])
            longitude = str(lat_lng[1])
            
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        title = soup.find("div",class_="title").find("h1").text.strip()
        item_loader.add_value("title", title)
        rent = soup.find("div",class_="title").find("h2", class_="price").text.strip()
        if rent:
            rent = int(float(rent.replace("€","").replace(" ","")))
            item_loader.add_value("rent", rent)

        ref =getSqureMtr(soup.find("section", class_="main show").find("article").find("p",class_="comment").find("span",class_="reference").text)
        item_loader.add_value("external_id", str(ref))

        desc = soup.find("section", class_="main show").find("article").find("p",class_="comment").text.strip()
        item_loader.add_value("description", desc)

        if "garage" in desc.lower() or "parking" in desc.lower() or "autostaanplaat" in desc.lower():
            item_loader.add_value("parking", True)
        if "terras" in desc.lower() or "terrace" in desc.lower():
            item_loader.add_value("terrace", True)
        if "balcon" in desc.lower() or "balcony" in desc.lower():
            item_loader.add_value("balcony", True)
        if "zwembad" in desc.lower() or "swimming" in desc.lower():
            item_loader.add_value("swimming_pool", True)
        if "gemeubileerd" in desc.lower() or "furnished" in desc.lower() or "meublé" in desc.lower():
            item_loader.add_value("furnished", True)
        if "machine à laver" in desc.lower():
            item_loader.add_value("washing_machine", True)
        if "lave" in desc.lower() and "vaisselle" in desc.lower():
            item_loader.add_value("dishwasher", True)
        if "lift" in desc.lower() or "ascenseur" in desc.lower():
            item_loader.add_value("elevator", True)

        detail_head = []
        all_head = soup.find("div", class_="details clearfix").find_all("div")
        for ech_head in all_head:
            detail_head.append(ech_head.find("h3").text.strip().lower())

        temp_dic = {}
        if "résumé" in detail_head:
            summary = soup.find("div", class_="details clearfix").find("div",class_="summary").find_all("li")
            for ech_li in summary:
                temp_dic[ech_li.text.replace(ech_li.find("span").text,"").strip()]=ech_li.find("span").text.strip()

            temp_dic = cleanKey(temp_dic)
            if "pi_ces" in temp_dic and getSqureMtr(temp_dic["pi_ces"]):
                item_loader.add_value("room_count", getSqureMtr(temp_dic["pi_ces"]))
            if "surface" in temp_dic and getSqureMtr(temp_dic["surface"]):
                item_loader.add_value("square_meters", getSqureMtr(temp_dic["surface"]))
            if "etage" in temp_dic:
                item_loader.add_value("floor", temp_dic["etage"])
            # if "disponiblele" in temp_dic:
            #     if num_there(temp_dic["disponiblele"]):
            #         date = temp_dic["disponiblele"]+"20"
            #         item_loader.add_value("available_date", format_date(date))

        available_date = response.xpath("//li[contains(.,'Disponible')]/span/text()").get()
        if available_date:
            if available_date.replace(" ","").isalpha() != True:
                date_parsed = dateparser.parse(available_date)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            elif "immédiatement" in available_date.lower():
                today = datetime.today().strftime('%Y-%m-%d')
                item_loader.add_value("available_date", today)
        
        deposit = response.xpath("//li[contains(.,'Dépôt ')]/span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ","").split("€")[0])

        address = response.xpath("//p[@class='comment']/text()[1]").get()
        if address:
            address = address.lower().strip().strip("-")
            item_loader.add_value("address", address.strip())
            zipcode = ""
            for i in address.split(" "):
                if i.isdigit():
                    zipcode = i
            if len(zipcode) == 5:
                item_loader.add_value("zipcode", zipcode)
            else: zipcode = ""
            
            if zipcode: item_loader.add_value("city",address.split(zipcode)[-1].strip().capitalize())
            else: item_loader.add_value("city", address.split(" - ")[-1].replace(".","").strip().capitalize())

        utilities = response.xpath("//li[contains(.,'Charges')]/span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace(" ","").split("€")[0])

        if "informations légales" in detail_head:
            legal = soup.find("div", class_="details clearfix").find("div", class_="legal").find_all("li")
            temp_dic1 = {}
            for ech_li in legal:
                if ech_li.find("span"):
                    temp_dic[ech_li.text.replace(ech_li.find("span").text,"").strip()] = ech_li.find("span").text
            temp_dic1 = cleanKey(temp_dic1)

            # if "charges" in temp_dic and getPrice(temp_dic["charges"]):
            #     item_loader.add_value("utilities", getPrice(temp_dic["charges"]))
            # if "d_p_tdegarantie" in temp_dic and getPrice(temp_dic["d_p_tdegarantie"]):
            #     item_loader.add_value("deposit", getPrice(temp_dic["d_p_tdegarantie"]))


        if "efficacité énergétique" in detail_head:
            energy = soup.find("div", class_="diagnostics").find("img",alt="Énergie - Consommation conventionnelle")["src"].split("/")[-1]
            energy = energy.replace("%2C",".")
            item_loader.add_value("energy_label", energy+" kWhEP/m².an")

        if "prestations" in detail_head:
            services = soup.find("div",class_="services").find_all("li")
            for ech_ser in services:
                facilities= ech_ser.text.strip()
                if "garage" in facilities.lower() or "parking" in facilities.lower() or "autostaanplaat" in facilities.lower():
                    
                    item_loader.add_value("parking", True)
                if "terras" in facilities.lower() or "terrace" in facilities.lower():
                    
                    item_loader.add_value("terrace", True)
                if "balcon" in facilities.lower() or "balcony" in facilities.lower():
                    
                    item_loader.add_value("balcony", True)
                if "zwembad" in facilities.lower() or "swimming" in facilities.lower():
                    
                    item_loader.add_value("swimming_pool", True)
                if "gemeubileerd" in facilities.lower() or "furnished" in facilities.lower() or "meublé" in facilities.lower():
                    
                    item_loader.add_value("furnished", True)
                if "machine à laver" in facilities.lower():
                  
                    item_loader.add_value("washing_machine", True)
                if "lave" in facilities.lower() and "vaisselle" in facilities.lower():
                   
                    item_loader.add_value("dishwasher", True)
                if "lift" in facilities.lower() or "ascenseur" in facilities.lower():
                 
                    item_loader.add_value("elevator", True)

        images_list = []
        if soup.find("div", class_="show-carousel owl-carousel owl-theme"):
            imgs = soup.find("div", class_="show-carousel owl-carousel owl-theme").find_all("div", class_="item resizePicture")
            for im in imgs:
                images_list.append(im.find("a")["href"])

            if images_list:
                item_loader.add_value("images", images_list)

        item_loader.add_value("landlord_name", "Régie Saint-Louis")
        item_loader.add_value("external_source", "regiesaintlouis_fr_PySpider_france_fr")
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("landlord_email", "location@saintlouis.immo")
        item_loader.add_value("landlord_phone", "+33472845205")
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        yield item_loader.load_item()