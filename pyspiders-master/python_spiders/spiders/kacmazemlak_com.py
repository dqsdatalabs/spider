# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags 
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import re
class MySpider(Spider):
    name = 'kacmazemlak_com'
    start_urls = ['https://www.kacmazemlak.com/emlak-kiralik'] 
    execution_type='testing'
    country='turkey'
    locale='tr' # LEVEL 1

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.kacmazemlak.com/emlak-kiralik-daire",
                    "https://www.kacmazemlak.com/emlak-kiralik-dubleks-daire",
                    "https://www.kacmazemlak.com/emlak-kiralik-esyali-mobleli-daire",
                    "https://www.kacmazemlak.com/emlak-kiralik-residence-rezidans",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.kacmazemlak.com/emlak-kiralik-villa",
                    "https://www.kacmazemlak.com/emlak-kiralik-yazlik-daire",
                    "https://www.kacmazemlak.com/emlak-kiralik-mustakil-ev",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.kacmazemlak.com/emlak-kiralik-studyo-daire",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(url=item,
                                    callback=self.parse,
                                    meta={'property_type': url.get('property_type'), "url":item})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'ilanlist-bg')]//div[contains(@class,'ilanlist-baslik-2')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if seen or page == 2:
            p_url = response.meta.get("url") + f"/{page}"
            yield Request(
                url=p_url,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type'), "page":page+1, "url": response.meta.get("url")}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_xpath("title", "//h1[@id='ilandetay_baslik']/text()")
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Kacmazemlak_PySpider_"+ self.country + "_" + self.locale)

        prop_type = response.meta.get('property_type')
        item_loader.add_value("property_type", prop_type)

        address = response.xpath("//li[contains(@class,'kacmaz-laci')]/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
        
        city = response.xpath("//li[@class='ioz-row d-block mb-2 pb-3 text-kacmaz-laci']/text()").get()
        if city:
            city = city.split('/')[0].strip()
            item_loader.add_value("city", city)
            
        latitude_longitude = response.xpath("//script[contains(.,'$tem_geo_lat')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('$tem_geo_lat="')[1].split('"')[0].strip()
            longitude = latitude_longitude.split('$tem_geo_lng="')[1].split('"')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
            
        description = "".join(response.xpath("//div[@id='readmore-ilan-aciklama']//p//text()").getall())    
        if description:            
            item_loader.add_value("description", description)   
            if "teraslı" in description.lower():
                item_loader.add_value("terrace",True)
            if "eşyasız" in description.lower() and "eşyalı" not in description.lower():
                item_loader.add_value("furnished",False)
            if "asansör" in description.lower():
                item_loader.add_value("elevator",True)
            if "otopark" in description.lower():
                item_loader.add_value("parking",True)
            if "çamaşır makinesi" in description.lower():
                item_loader.add_value("washing_machine",True)
            if "yüzme havuzu" in description.lower():
                item_loader.add_value("swimming_pool",True)
        square_meters = response.xpath("//span[contains(.,') M')]/parent::div/parent::li/div[2]/text()").get()
        if square_meters:
            square_meters = square_meters.strip().split(' ')[0]
            item_loader.add_value("square_meters", square_meters)
        bath_room = response.xpath("//span[contains(.,'Banyo')]/parent::div/parent::li/div[2]/text()").get()
        if bath_room:
            item_loader.add_value("bathroom_count", bath_room)

        utilities = response.xpath("//span[contains(.,'Aidat')]/parent::div/parent::li/div[2]/text()").get()
        if utilities:            
            item_loader.add_value("utilities", utilities.replace("TL","").strip())

        room_count = response.xpath("//span[contains(.,'Oda') or contains(.,'Bölüm Sayısı')]/parent::div/parent::li/div[2]/text()").get()
        if room_count:
            if "Stüdyo" in room_count:
                room_count = "1"
            elif room_count.strip()[-1] == '+':
                room_count = room_count.strip().split('+')[0]
            elif len(room_count.split('+')) > 1:
                room_count = int(room_count.split('+')[0].strip())+int(room_count.split('+')[1])
            else:
                room_count = room_count.strip().split(' ')[0]
            item_loader.add_value("room_count", room_count)
        elif description and "+" in description:
            try:
                room1 = description.split("+")[0].strip().split(" ")[-1].strip()
                room2 = description.split("+")[1].strip().split(" ")[0].strip()
                if room1.isdigit() and room2.isdigit():
                    item_loader.add_value("room_count",int(room1)+int(room2))
            except:
                pass
        rent = response.xpath("//span[@class='ilan-fiyat-text ']/text()").get()
        if rent:
            rent1 = rent.strip().split(' ')[2].replace(',', '.')
            if "Haftalık" in rent:
                    rent1 = int(rent1.replace(".",""))*4   
            elif "Günlük" in rent:
                    rent1 = int(rent1.replace(".",""))*30     
            item_loader.add_value("rent", rent1)
        
        currency = 'TRY'
        item_loader.add_value("currency", currency)

        external_id = response.xpath("//div[@id='ilandetay_emlakno']/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)
    
        description = response.xpath("//div[@id='readmore-ilan-aciklama']//p//text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d + ' '
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)        

        images = []
        for x in response.xpath("//div[@class='row ml-auto mr-auto']//img/@src").getall():
            if x != '':
                images.append(x)
        for x in response.xpath("//div[@class='row ml-auto mr-auto']//img/@data-src").getall():
            if x != '':
                images.append(x)
        if images:
            item_loader.add_value("images", list(set(images)))
            item_loader.add_value("external_images_count", str(len(images)))

        furnished = response.xpath("//li[contains(.,'Mobilyalı')] | //h1[@id='ilandetay_baslik']/text()[contains(.,'Eşyalı')]").get()
        if furnished:
            furnished = True
            item_loader.add_value("furnished", furnished)

        floor = response.xpath("//span[contains(.,'Bulunduğu Kat')]/parent::div/parent::li/div[2]/text()").get()
        if floor:
            try:
                if ". Kat" in floor:
                    floor = floor.split(".")[0]
            except:
                pass
            item_loader.add_value("floor", floor.strip())

        parking = response.xpath("//li[contains(.,'Otopark')]").get()
        if parking:
            parking = True
            item_loader.add_value("parking", parking)

        elevator = response.xpath("//li[contains(.,'Asansör')]").get()
        if elevator:
            elevator = True
            item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//li[contains(.,'Balkon')]").get()
        if balcony:
            balcony = True
            item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//li[contains(.,'Teras')]").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)

        swimming_pool = response.xpath("//li[contains(.,'Havuz')]").get()
        if swimming_pool:
            swimming_pool = True
            item_loader.add_value("swimming_pool", swimming_pool)

        result = response.xpath("//li[contains(.,'Beyaz Eşya')]").get()
        if result:
            item_loader.add_value("washing_machine", True)
            item_loader.add_value("dishwasher", True)

        landlord_name = response.xpath("//div[@class='d-flex justify-content-center p-2 navbar-default-bs3 temsilci-user']/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//span[@class='glyphicon glyphicon-phone']/parent::div/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        
        landlord_email = response.xpath("//input[@id='ilan_epg_mesaj_alt']/@value[contains(.,'@')]").get()
        if landlord_email:
            landlord_email = landlord_email.split("<br/>")[-1].strip()
            item_loader.add_value("landlord_email", landlord_email)
        else:
            item_loader.add_value("landlord_email", "acibadem@kacmazemlak.com")
        
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data