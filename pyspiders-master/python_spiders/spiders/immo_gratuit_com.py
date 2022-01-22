# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re 

class MySpider(Spider): 
    name = 'immo_gratuit_com'
    execution_type='testing'
    country='france'
    locale='fr' 
    external_source="Immo_Gratuit_PySpider_france"
    def start_requests(self):
         
        start_urls = [
            {
                "url" : [
                    "http://www.immo-gratuit.com/annonces.php?page=1&annonce=L&type=AP&departement=&ville=",
                    "http://www.immo-gratuit.com/annonces.php?page=1&annonce=L&type=DU&departement=&ville=",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "http://www.immo-gratuit.com/annonces.php?page=1&annonce=L&type=MA&departement=&ville=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "http://www.immo-gratuit.com/annonces.php?page=1&annonce=L&type=ST&departement=&ville=",
                ],
                "property_type" : "studio"
            },
            {
                "url" : [
                    "http://www.immo-gratuit.com/annonces.php?page=1&annonce=L&type=CH&departement=&ville=",
                ],
                "property_type" : "room"
            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//strong[contains(.,'Loue')]/../@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(.,'Page suivante')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source",self.external_source)
        
        title = response.xpath("//title/text()").get()
        if title:
            title=title.replace("Immo-gratuit","")
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        item_loader.add_value("external_id", response.url.split(".html")[0].split("--")[-1])

        filt = response.xpath("//title/text()[contains(.,'marrakech') or contains(.,'Marrakech')] | //strong[contains(.,'Description')]/following-sibling::text()[1][contains(.,'marrakech') or contains(.,'Marrakech')]")
        if filt: return

        year_filt = response.xpath("//b[contains(.,'annonce parue le')]/text()").get()
        if year_filt:
            year_filt = year_filt.strip().split(" ")[-1].strip()
            if int(year_filt) > 2020:
        
                status = response.xpath("//a[contains(.,'Var')]/text()").get()
                if status:
                    return
                address = "".join(response.xpath("//div//tr/td[contains(.,'Ville')]/following-sibling::td/text()").getall())
                if address:
                    item_loader.add_value("address", address.strip())
                    item_loader.add_value("city", address.strip())
                
                zipcode = response.xpath("//div//tr/td[contains(.,'Code')]/following-sibling::td/text()").get()
                if zipcode:
                    item_loader.add_value("zipcode", zipcode)
                
                square_meters = response.xpath("//div//tr/td[contains(.,'Surface')]/following-sibling::td/text()").get()
                if square_meters:
                    square_meters = square_meters.split(" ")[0]
                    if square_meters != "0":
                        item_loader.add_value("square_meters", square_meters)
                
                room_count = response.xpath("//div//tr/td[contains(.,'pièce')]/following-sibling::td/text()").get()
                if room_count:
                    room_count = room_count.split(" ")[0]
                    if room_count != "0": 
                        item_loader.add_value("room_count", room_count)
                
                rent = response.xpath("//div//tr/td[contains(.,'Prix')]/following-sibling::td//text()").get()
                if rent:
                    price = rent.split("€")[0].strip().replace(" ","").split(",")[0]
                    if price.isdigit():
                        if int(price) < 10000:
                            item_loader.add_value("rent", price)
                            item_loader.add_value("currency", "EUR")
                    
                description = response.xpath("//div/strong[contains(.,'Description')]/following-sibling::text()").get()
                if description:
                    desc = re.sub('\s{2,}', ' ', description.replace(">","").strip())
                    item_loader.add_value("description", desc)
                
                energy_label = response.xpath("//div/img/@src[contains(.,'dpe')]").get()
                if energy_label:
                    energy_label = energy_label.split("val=")[1].strip()
                    item_loader.add_value("energy_label", energy_label_calculate(energy_label))
                 
                latitude_longitude = response.xpath("//script[contains(.,'map')]/text()").get()
                if latitude_longitude:
                    latitude = latitude_longitude.split("setView([")[1].split(",")[0]
                    longitude = latitude_longitude.split("setView([")[1].split(",")[1].split("]")[0].strip()
                    item_loader.add_value("latitude", latitude)
                    item_loader.add_value("longitude", longitude)
                
                images = [x for x in response.xpath("//tr/td/a/@href[contains(.,'jpg')]").getall()]
                if images:
                    item_loader.add_value("images", images)
                
                landlord_name = response.xpath("//strong[contains(.,'Annonce de ')]/following-sibling::text()[1]").get()
                if landlord_name: item_loader.add_value("landlord_name", landlord_name)
                
                landlord_phone = response.xpath("//div/strong[contains(.,'Description')]/parent::div/text()[contains(.,'Téléphone')]").get()
                if landlord_phone: item_loader.add_value("landlord_phone", landlord_phone.split(":")[-1].strip().replace("-", " "))
                else:
                    landlord_phone = response.xpath("//div/strong[contains(.,'Description')]/parent::div/text()[contains(.,'Portable')]").get()
                    if landlord_phone: item_loader.add_value("landlord_phone", landlord_phone.split(":")[-1].strip().replace("-", " "))
                
                landlord_email = response.xpath("//div/strong[contains(.,'Description')]/parent::div/text()[contains(.,'Mail')]").get()
                if landlord_email: item_loader.add_value("landlord_email", landlord_email.split(":")[1].strip())

                item_loader.add_value("landlord_name", "Immo Gratuit")

                from datetime import datetime 
                import dateparser
                from datetime import date 

                available_date = "".join(response.xpath("//center/font[@class='option']/b/text()").get()).split("le")[1].strip()
                if available_date: 

                   date_parsed = dateparser.parse(available_date, date_formats=["%Y/%m/%d"])                 
                   date2 = date_parsed.strftime("%Y-%m-%d")
                   date1=datetime.now()
                   date1=datetime.strftime(date1,"%Y-%m-%d")
                   yil,ay,gun=date2.split("-")

                
                   dateretrict=(date.today()-date(int(yil),int(ay),int(gun))).days
                   if dateretrict>30:return 
                   else:item_loader.add_value("available_date",date2)
                        

                       
                
                
                yield item_loader.load_item()



def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number == 0:
        return
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label