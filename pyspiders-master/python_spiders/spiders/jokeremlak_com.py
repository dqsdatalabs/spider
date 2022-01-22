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
    name = 'jokeremlak_com'
    execution_type = 'testing'
    country = 'turkey'
    locale = 'tr' 

    def start_requests(self):
        start_urls = [
            {
                "url" : "http://www.jokeremlak.com/ara/?post=emlakara&d=&l=&g=&tip=kiralik&anakategori=7&kategori=2&il=&minmetrekarem=&maxmetrekarem=&otopark=&minfiyat=&maxfiyat=",
                "property_type" : "apartment"
            },
            {
                "url" : "http://www.jokeremlak.com/ara/?post=emlakara&d=&l=&g=&tip=kiralik&anakategori=7&kategori=1&il=&minmetrekarem=&maxmetrekarem=&otopark=&minfiyat=&maxfiyat=",
                "property_type" : "house"
            },
            {
                "url" : "http://www.jokeremlak.com/ara/?post=emlakara&d=&l=&g=&tip=kiralik&anakategori=7&kategori=19&il=&minmetrekarem=&maxmetrekarem=&otopark=&minfiyat=&maxfiyat=",
                "property_type" : "house"
            },
        ]# LEVEL 1
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    def parse(self, response):
        
        for item in response.xpath("//div[contains(@class,'uk-card')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta = {"property_type":response.meta.get("property_type")})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Jokeremlak_PySpider_" + self.country + "_" + self.locale)

        rented = response.xpath("//span[contains(.,'KİRALANDI')]/text()").get()
        if rented: return

        item_loader.add_value("external_link", response.url)
        title = response.xpath("//h5[@class='title']/text()").get()
        if title:
            item_loader.add_value("title", title)
        room_count = response.xpath("//td[contains(.,'Oda Sayısı')]/following-sibling::td/text()").extract_first()
        
        item_loader.add_xpath("external_id", "//td[contains(.,'İlan No')]/following-sibling::td/text()")
        item_loader.add_value("property_type", response.meta.get("property_type"))

        desc = "".join(response.xpath("//div[@id='aciklama']//p//text()").extract())
        if desc:
            item_loader.add_value("description", desc)
            if "Kedi beslemek isteyenler için" in desc:
                item_loader.add_value("pets_allowed", True)

        
        square_meters = response.xpath("//td[contains(.,'Metre')]/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip())
        elif not square_meters and desc:
            try:
                unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(m²|meters2|metres2|meter2|metre2|mt2|m2|M2)",desc)
                if unit_pattern:
                    sq=int(float(unit_pattern[0][0]))
                    item_loader.add_value("square_meters", str(sq))
            except:
                pass

        item_loader.add_xpath("floor", "//td[contains(.,'Bulunduğu Kat')]/following-sibling::td/text()")

        rent = response.xpath("//td[contains(.,'Fiyat')]/following-sibling::td/text()").get()
        if rent:
            rent = rent.replace(".","").strip()
            item_loader.add_value("rent", rent)

        item_loader.add_value("currency", "TRY")
        
        address = " ".join(response.xpath("//div[@class='ev-details']/p[@class='ev-location']/text()").extract())
        address2=response.xpath("//article/p/text()").get()
        if address:
            item_loader.add_value("address",address.strip())
            item_loader.add_value("city",address.split("/")[0].strip())

        if room_count:
            if "+" in room_count :
                count1 = room_count.split("+")[0].strip()
                count2 = room_count.split("+")[1].replace("Stüdyo","").strip()
                sub = int(count1)+int(count2)
                item_loader.add_value("room_count",str(sub))
            else:
                item_loader.add_value("room_count",room_count)
        
        bath_room = response.xpath("//td[contains(.,'Banyo')]/following-sibling::td/text()").extract_first()
        if bath_room:
            item_loader.add_value("bathroom_count", bath_room.strip())       
        
        utilities = response.xpath("//td[contains(.,'Aidat')]/following-sibling::td/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.strip())    
        
        images = [response.urljoin(x)for x in response.xpath("//ul[@class='uk-slideshow-items']//@src").extract()]
        if images:
                item_loader.add_value("images", images)
            
        furnished = response.xpath("//td[contains(.,'Eşyalı')]/following-sibling::td/text()").get()
        if furnished:
            if "evet" in furnished.lower():
                item_loader.add_value("furnished", True)
            elif "hayır" in furnished.lower():
                item_loader.add_value("furnished", False)
        elif response.xpath("//li/i[contains(@class,'check')]/following-sibling::text()[contains(.,'Mobilya')]").get():
            item_loader.add_value("furnished", True)
            
        parking = response.xpath("//td[contains(.,'Otopark')]/following-sibling::td/text()").get()
        if parking:
            if "evet" in parking.lower() or "var" in parking.lower():
                item_loader.add_value("parking", True)
            elif "yok" in parking.lower():
                item_loader.add_value("parking", False)
        elif not parking:
                parking = response.xpath("//li/i[contains(@class,'check')]/following-sibling::text()[contains(.,'Otopark')]").get()
                if parking:
                    item_loader.add_value("parking", True)
                    
        terrace = "".join(response.xpath("//li/i[contains(@class,'check')]/following-sibling::text()[contains(.,'Teras')]").extract())
        if terrace:
            item_loader.add_value("terrace", True)

        terrace = "".join(response.xpath("//li/i[contains(@class,'check')]/following-sibling::text()[contains(.,'Balcon')]").extract())
        if terrace:
            item_loader.add_value("balcony", True)

        terrace = "".join(response.xpath("//li/i[contains(@class,'check')]/following-sibling::text()[contains(.,'Çamaşır Makinesi')]").extract())
        if terrace:
            item_loader.add_value("washing_machine", True)

        terrace = "".join(response.xpath("//li/i[contains(@class,'check')]/following-sibling::text()[contains(.,'Bulaşık Mak')]").extract())
        if terrace:
            item_loader.add_value("dishwasher", True)

        landlord_phone = response.xpath("//h5[contains(.,'Danışman')]/../div/i[contains(@class,'phone')]/parent::div/text()").get()
        if landlord_phone: item_loader.add_value("landlord_phone", landlord_phone.strip())
        item_loader.add_value("landlord_email", "jokeremlak@gmail.com")
        item_loader.add_xpath("landlord_name", "//h5[contains(.,'Danışman')]/../p/text()")
        
        map_coord = response.xpath("//script[@type='text/javascript']/text()[contains(.,'center')]").extract_first()
        if map_coord:
            lat = map_coord.split("center: [")[1].split(",")[0].strip()
            lng = map_coord.split(",")[1].split("],")[0].strip()
            if lat and lng:
                item_loader.add_value("longitude", lat.strip())
                item_loader.add_value("latitude", lng.strip())
        
        yield item_loader.load_item()