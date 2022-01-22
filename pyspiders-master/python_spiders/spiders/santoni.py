# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from bs4 import BeautifulSoup
import requests
from ..loaders import ListingLoader
from ..items import ListingItem
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
# import geopy
# from geopy.geocoders import Nominatim
# from geopy.extra.rate_limiter import RateLimiter

# locator = Nominatim(user_agent="myGeocoder")

# def getAddress(lat,lng):
#     coordinates = str(lat)+","+str(lng) 
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

class QuotesSpider(scrapy.Spider):
    name = "santoni_fr_PySpider_france_fr"
    allowed_domains = ['www.santoni.fr']
    start_urls = ['www.santoni.fr']
    execution_type = 'testing'
    country = 'france'
    locale ='fr'

    def start_requests(self):
        url = 'https://www.santoni.fr/fr/liste.htm?page=1&menuSave=2&TypeModeListeForm=text&vlc=4&LibMultiType=Tous+types+de+bien&lieu-alentour=0'
        
        yield scrapy.Request(
            url=url,
            callback=self.parse)


    def parse(self, response):
        for item in response.xpath("//a[contains(@itemprop,'url')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.get_property_details)
        
        next_page = response.xpath("//div[contains(@class,'liste-navpage-next')]//@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)

      

    def get_property_details(self, response):
        item = ListingItem()
        item_loader = ListingLoader(response=response)

        external_link = response.url
        print(external_link)
        
        
        soup2 = BeautifulSoup(response.body)
        property_type = response.xpath("//li[contains(@class,'type')]//text()").get()
        if get_p_type_string(property_type): 
            item['property_type'] = get_p_type_string(property_type)
        else: return
        address = soup2.find("div",{"class":"detail-bien-specs"}).find("li",{"class":"detail-bien-ville"}).text.strip()
        city = address.split('(')[0]
        rent = soup2.find("div",{"class":"detail-bien-prix"}).text
        description = soup2.find("div",{"class":"detail-bien-desc-content clearfix"}).find("p").text.strip()

        zipcode = response.xpath("//li[@class='detail-bien-ville']/h2/text()").get()
        if zipcode: item["zipcode"] = zipcode.split("(")[-1].split(")")[0].strip()
        
        images = []
        for img in soup2.findAll("div",{"class":"diapo is-flap"}):
            images.append(img.find("div", {"class" : "bg-blur-image"}).find("img").get("data-src").split('?')[0])
        if images:
            item["images"]= images
            item["external_images_count"]= len(images)
            
        # location = getAddress(soup2.find("li", {"class": "gg-map-marker-lat"}).text,soup2.find("li", {"class": "gg-map-marker-lng"}).text)
        item["latitude"] = soup2.find("li", {"class": "gg-map-marker-lat"}).text.strip()
        item["longitude"] = soup2.find("li", {"class": "gg-map-marker-lng"}).text.strip()

        temp_dic = {}
        all_li = soup2.findAll("div", {"class" : "detail-infos-sup"})
        for al in all_li:
            for l in al.findAll("li"):
                all_span = l.findAll("span")
                if len(all_span) == 2:
                    key = all_span[0].text
                    val = all_span[1].text
                    temp_dic[key] = val

        temp_dic = cleanKey(temp_dic)

        if "kosten" in temp_dic:
            text_list = re.findall('\d+',temp_dic["kosten"])
            if int(text_list[0]):
                item["utilities"]=int(text_list[0])

        if "gemeubeld" in temp_dic and temp_dic["gemeubeld"] == "ja":
            item["furnished"]=True
        elif "gemeubeld" in temp_dic and temp_dic["gemeubeld"] == "nee":
            item["furnished"]=False

        if "lift" in temp_dic and temp_dic["lift"] == "ja":
            item["elevator"]=True
        elif "lift" in temp_dic and temp_dic["lift"] == "nee":
            item["elevator"]=False

        if "verdieping" in temp_dic:
            item["floor"]=temp_dic["verdieping"]

        if "balkon" in temp_dic and temp_dic["balkon"] == "ja":
            item["balcony"]=True
        elif "balkon" in temp_dic and temp_dic["balkon"] == "nee":
            item["balcony"]=False

        if "salled_eau" in temp_dic and getSqureMtr(temp_dic["salled_eau"]):
            item["bathroom_count"]=int(str(getSqureMtr(temp_dic["salled_eau"])))
        elif "salledebain" in temp_dic and getSqureMtr(temp_dic["salledebain"]):
            item["bathroom_count"] = int(re.findall('\d+',temp_dic["salledebain"])[0])

        if "garage" in description.lower() or "parking" in description.lower():
            item["parking"] = True
        if "terras" in description.lower():
            item["terrace"] = True
        if "zwembad" in description.lower() or "swimming" in description.lower():
            item["swimming_pool"] = True
        if "gemeubileerd" in description.lower()or "aménagées" in description.lower() or "furnished" in description.lower():
            item["furnished"]=True
        if "garage" in description.lower() or "parking" in description.lower():
            item["parking"] = True


        if "consommation-" in soup2.find("div", {"class" : "detail-bien-dpe clearfix"}).findAll("img")[0].get("src"):
            item["energy_label"] = soup2.find("div", {"class" : "detail-bien-dpe clearfix"}).findAll("img")[0].get("src").split('consommation-')[1].split('.')[0] + ' kWhEP/m2'
        

        if soup2.find("span",class_="cout_charges_mens") and num_there(soup2.find("span",class_="cout_charges_mens").text):
            item["utilities"] = getSqureMtr(soup2.find("span",class_="cout_charges_mens").text)

        if "de garantie:" in description:
            deposit = description.split("de garantie:")[1].strip().split(" ")[0]
            item["deposit"] = deposit.replace("€","")
        elif soup2.find("span",class_="cout_honoraires_loc") and num_there(soup2.find("span",class_="cout_honoraires_loc").text):
            item["deposit"] = getSqureMtr(soup2.find("span",class_="cout_honoraires_loc").text)
        
        title = soup2.find("h1", {"class" : "side-detail-titre"}).text
        try:
            item["room_count"]= int(re.findall(r'\d+', soup2.find("h1", {"class" : "side-detail-titre"}).text.split(',')[1])[0])
        except:
            if "Garage," in title:
                return
        item["currency"]='EUR'
        item["external_link"] = external_link
        item["city"] = city
        item["address"] = address
        item["title"] = title
        item["rent"] = int(re.findall(r'\d+', rent)[0])
        item["description"] = soup2.find("div",{"class":"detail-bien-desc-content clearfix"}).find("p").text.strip()
        item["square_meters"] = int(re.findall('\d+',temp_dic["habitable"])[0])        
        item["external_source"] = 'santoni_fr_PySpider_france_fr'
        item["external_id"] = soup2.findAll("span",itemprop="productID")[1].text.strip()
        item["landlord_name"] = "S'ANTONI Immobilier"
        item["landlord_email"] = "infocontact@santoni.fr"
        item["landlord_phone"] = soup2.find("li", itemprop="telephone").find("a").text.strip()

        yield item
def get_p_type_string(p_type_string):
    if p_type_string and "local" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("studio" in p_type_string.lower() or "t1" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "chambre" in p_type_string.lower():
        return "room"   
    else:
        return None
        




