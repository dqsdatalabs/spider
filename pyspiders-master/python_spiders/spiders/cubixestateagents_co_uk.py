# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import js2xml
from ..loaders import ListingLoader
from ..items import ListingItem
from scrapy import Request,FormRequest
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date
import re,json
from bs4 import BeautifulSoup
import requests
# import geopy
# from geopy.geocoders import Nominatim

# geolocator = Nominatim(user_agent="myGeocoder")

# def get_lat_lon(_address):
#     location = geolocator.geocode(_address)
#     return location.latitude,location.longitude


# def getAddress(lat,lng):
#     coordinates = str(lat)+","+str(lng)
#     location = geolocator.reverse(coordinates)
#     return location

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


class QuotesSpider(scrapy.Spider):
    name = 'cubixestateagents_co_uk'
    allowed_domains = ['www.cubixestateagents.co.uk']
    start_urls = ['www.cubixestateagents.co.uk']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'

    # def start_requests(self):
    #     url="https://www.cubixestateagents.co.uk/properties-for-rent/?tab=for-rent"

    #     yield scrapy.Request(
    #         url=url,
    #         callback=self.parse
    #         )
    def start_requests(self):
        # start_urls = [
        #     {
        #         "url" : [
        #             "https://cubixestateagents.co.uk/search-results/?keyword=&location%5B%5D=&status%5B%5D=for-rent&type%5B%5D=flat-apartment&bedrooms=&bathrooms=&min-area=&max-area=&min-price=&max-price=&property_id=&label%5B%5D=",
        #         ],
        #         "property_type" : "apartment"
        #     },
        #     {
        #         "url" : [
        #             "https://cubixestateagents.co.uk/search-results/?keyword=&location%5B%5D=&status%5B%5D=for-rent&type%5B%5D=maisonette&type%5B%5D=penthouse&bedrooms=&bathrooms=&min-area=&max-area=&min-price=&max-price=&property_id=&label%5B%5D=",
        #         ],
        #         "property_type" : "house"
        #     },   
        #     {
        #         "url" : [
        #             "https://cubixestateagents.co.uk/search-results/?keyword=&location%5B%5D=&status%5B%5D=for-rent&type%5B%5D=studio&bedrooms=&bathrooms=&min-area=&max-area=&min-price=&max-price=&property_id=&label%5B%5D=",
        #         ],
        #         "property_type" : "studio"
        #     }, 
        # ]
        # for url in start_urls:
        #     for item in url.get("url"):
        #         yield Request(item,
        #                     callback=self.parse,
        #                     meta={'property_type': url.get('property_type')})

        url = "https://cubixestateagents.co.uk/property/1-room-available-in-6-bedroom-flat-share-with-garden-harmon/"
        yield Request(url,
                            callback=self.parse,
                            meta={'property_type': "aaaa"})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 24)
        # total_result = int(response.xpath("//span[@class='searchHeader-resultCount']/text()").get().strip())
 
        for item in response.xpath("//div[@class='item-body flex-grow-1']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.get_property_details,dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        page = response.xpath("//div[@class='pagination-wrap']//ul/li[last()-1]/a[@class='page-link']/@href").extract_first()
        if page:
            yield Request(page,callback=self.parse,dont_filter=True, meta={'property_type': response.meta.get('property_type')})


    def get_property_details(self, response, **kwargs):

        item = ListingItem()
        soup = BeautifulSoup(response.body,"html.parser")
        str_soup = str(soup)


        match = re.findall("var houzez_single_property_map =(.+);",str_soup)
        if match:
            dic_data = eval(match[0])
            if "lat" in dic_data:
                item["latitude"] = str(dic_data["lat"])
            if "lng" in dic_data:
                item["longitude"] = str(dic_data["lng"]) 

        title = soup.find("div", class_="page-title").find("h1").text.strip()
        item["title"] = title

        desc = soup.find("div", id="property-description").find("div", class_="block-content-wrap").text.strip()
        item["description"] = desc
        # print(desc)

        if "garage" in desc.lower() or "parking" in desc.lower() or "autostaanplaat" in desc.lower():
            item["parking"] = True
        if "terras" in desc.lower() or "terrace" in desc.lower():
            item["terrace"] = True
        if "balcon" in desc.lower() or "balcony" in desc.lower():
            item["balcony"] = True
        if "zwembad" in desc.lower() or "swimming" in desc.lower():
            item["swimming_pool"] = True
        if ("gemeubileerd" in desc.lower() or "furnished" in desc.lower() or "meublé" in desc.lower()) and "unfurnished" not in desc.lower():
            item["furnished"] = True
        if "machine à laver" in desc.lower() or ("washing" in desc.lower() and "machine" in desc.lower()):
            item["washing_machine"] = True
        if ("lave" in desc.lower() and "vaisselle" in desc.lower()) or "dishwasher" in desc.lower():
            item["dishwasher"] = True
        if "lift" in desc.lower() or "ascenseur" in desc.lower() or "elevator" in desc.lower():
            item["elevator"] = True

        features = soup.find('div',id='property-features-wrap').findAll('li')
        for f in features:
            if 'Parking' in f.get_text() or 'parking' in f.get_text():
                item["parking"]=True 

            if 'Lift' in f.get_text():
                item["elevator"]=True     

        temp_dic = {}
        details = soup.find("div",id="property-details").find("div", class_="detail-wrap").find("ul").find_all("li")
        for ech_det in details:
            temp_dic[ech_det.find("strong").text] = ech_det.find("span").text
        temp_dic = cleanKey(temp_dic)
        # print(temp_dic)
        # print ("\n")
        
        if "propertyid" in temp_dic:
            item["external_id"] = temp_dic["propertyid"]
        
        # if "price" in temp_dic:
        #     # item["rent"] = getPrice(temp_dic["price"])
        #     item["rent"] = item["rent"] * 4

        price = ""
        rent = response.xpath("//div[@class='page-title-wrap']/div//ul/li[@class='item-sub-price']/text()").extract_first()
        if rent:
            if "pw" in rent.lower():
                price = rent.split("/")[0].replace("£","").strip()
                item["rent"] = int(price.replace(",","")) * 4
            elif "month" in rent.lower():
                rent_string = rent.replace(",","").split("/")[0].replace("£","").strip()
                item["rent"] = int(rent_string)
            

        if "propertysize" in temp_dic:
            item["square_meters"] = getSqureMtr(temp_dic["propertysize"])

        if "bedrooms" in temp_dic:
            item["room_count"] = int(temp_dic["bedrooms"])

        if "bedroom" in temp_dic:
            item["room_count"] = int(temp_dic["bedroom"])

        room= response.xpath("//section//div[@class='page-title']/h1/text()[contains(.,'Bedroom')]").extract_first()
        if room:
            room_count = room.split(" ")[0].split("Bedroom")[0].strip()
            if room_count.isdigit():
                item["room_count"] = int(room_count)

        if "bathroom" in temp_dic:
            item["bathroom_count"] = int(temp_dic["bathroom"])

        if "bathrooms" in temp_dic:
            item["bathroom_count"] = int(temp_dic["bathrooms"])


        property_type="NA"
        if "propertytype" in temp_dic:
            if "apartment" in temp_dic["propertytype"].lower():
                property_type = "apartment"
            elif "house" in temp_dic["propertytype"].lower() or "maisonette" in temp_dic["propertytype"].lower():
                property_type = "house"
            elif "room" in temp_dic["propertytype"].lower():
                property_type = "room"
            else:
                property_type = "NA"



        if soup.find("address",class_="item-address"):
            item["address"] = soup.find("address",class_="item-address").text.strip()
        else:
            address = " ".join(response.xpath("//div[@class='block-content-wrap']/ul[@class='list-2-cols list-unstyled']/li/span/text()").extract())
            if address:
                item["address"] = address.strip()

        item["address"] = response.xpath("//div[@class='block-content-wrap']/ul/li[@class='detail-zip']/span/text()").extract_first()

        temp_dic = {}
        add_detail = soup.find("div",id="property-address").find("div", class_="block-content-wrap").find("ul").find_all("li")
        for ech_add_det in add_detail:
            temp_dic[ech_add_det.find("strong").text] = ech_add_det.find("span").text
        temp_dic = cleanKey(temp_dic)

        if "address" in temp_dic:
            item["address"] = temp_dic["address"]
        if "city" in temp_dic:
            item["city"] = temp_dic["city"]
        if "zip_postalcode" in temp_dic:
            item["zipcode"] = temp_dic["zip_postalcode"]

        images = [x for x in response.xpath("//div[@class='top-gallery-section']//div[@id='property-gallery-js']/div//img/@src").getall()]
        if images:
            item["images"] = images

        item["external_source"] = "cubixestateagents_co_uk_PySpider_united_kingdom_en"
        item["landlord_phone"] = "0203 582 8710"
        item["landlord_email"] = "info@cubixestateagents.co.uk"
        item["landlord_name"] = "Cubix Estate Agents"
        item["currency"] = "GBP"
        item["external_link"] = response.url


        if property_type in ["apartment", "house", "room", "property_for_sale", "student_apartment", "studio"]:
            item["property_type"] = property_type
            yield item
