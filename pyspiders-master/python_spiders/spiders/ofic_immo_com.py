# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim

 

class MySpider(Spider):
    name = 'ofic_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    headers = {
        'content-type': "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW",
        'cache-control': "no-cache",
    }
    external_source="Oficimmo_PySpider_france_fr"
    def start_requests(self):
        start_urls = [
            {
                "s_type" : "location",
                "r_type" : "maison", 
                "property_type" : "house",
            },
            {
                "s_type" : "location",
                "r_type" : "appartement", 
                "property_type" : "apartment",
            },  
        ]
        for url in start_urls:
            s_type = url.get("s_type")
            r_type = url.get("r_type")
            payload = f"------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"property_search[_token]\"\r\n\r\nmOgpTjl4zi5wMm6GR6Qxv7Ydy1mWt5HRj4G11R96Ecg\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"property_search[typeTransac]\"\r\n\r\n{s_type}\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"property_search[type][]\"\r\n\r\n{r_type}\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"property_search[budgetMin]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"property_search[budgetMax]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"property_search[nbRoom]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"property_search[nbBedRoom]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"property_search[surfaceMin]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"property_search[ref]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW--"
            f_url = f"https://www.ofic-immo.com/fr/{s_type}/1/"
            yield Request(url=f_url,
                            callback=self.parse,
                            method="POST",
                            headers=self.headers,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'overlay-container')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", self.external_source)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        title=response.xpath("//h1[@class='font-lg']/text()").get()
        if title:
            item_loader.add_value("title",title)
        city=response.xpath("//span[@itemprop='addressLocality']/text()").get()
        if city:
            item_loader.add_value("city",city.split("(")[0].strip())
        
        
        
        rent=response.xpath("//span[@itemprop='price']/a/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        else: return
        
        square_meters=response.xpath("//div/ul/li[contains(.,'Surface')]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m²')[0].strip())
        else: return
        
        room_count=response.xpath("//div/ul/li[contains(.,'Pièce(s) : ')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else: return
        bathroom_count=response.xpath("//ul//li[contains(.,'Salle(s) de')]//span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        deposit=response.xpath("//ul//li[contains(.,'Dépôt')]//span/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0].strip())
        utility=response.xpath("//ul//li[contains(.,'charges ')]//span/text()").get()
        if utility:
            item_loader.add_value("utilities",utility.split(".")[0].split("€")[0])
        latitude_longitude = response.xpath("//script[contains(.,'mapboxgl.Map')]/text()").get()
        if latitude_longitude:
            longitude = latitude_longitude.split('center: [')[1].split(',')[0]
            latitude = latitude_longitude.split('center: [')[1].split(',')[1].split(']')[0]
            geolocator = Nominatim(user_agent=response.url)
            try:
                location = geolocator.reverse(latitude + ', ' + longitude, timeout=None)
                if location.raw['address']['postcode']:
                    zipcode = location.raw['address']['postcode']
                
            except:
                zipcode = None
            address=response.xpath("//span[@itemprop='addressLocality']/text()").get()
            if address:
                address=address.split("(")[0].strip()
                item_loader.add_value("address", address)
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
            else:
                return
        else:
            return

        external_id=response.xpath("//div/p[contains(.,'Ref:')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[1].strip())

        desc="".join(response.xpath("//p[@itemprop='description']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        energy_label=response.xpath(
            "//ul/li/span[contains(@class,'diagnostic__detail-result')]/parent::li/span[1]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        terrace=response.xpath("//div/ul/li[contains(.,'Terrasse')]/span/text()[contains(.,'OUI')]").get()
        if terrace:
            item_loader.add_value("terrace",True)
            
        parking=response.xpath("//div/ul/li[contains(.,'Parking')]/span/text()").get()
        if parking!='0':
            item_loader.add_value("parking",True)
  
        images=[x for x in response.xpath("//ul[contains(@class,'learing-thumbs clearing-feature')]/li/a/img/@src | //div[@class='overlay-container']/a/@href").getall()]
        for image in images:
            item_loader.add_value("images", "https://www.ofic-immo.com/"+image)
        item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name","OFIC IMMOBILIER")
        item_loader.add_value("landlord_phone","02.97.41.71.71")
  
        
        yield item_loader.load_item()

