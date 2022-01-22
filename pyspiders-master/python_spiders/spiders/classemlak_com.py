# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import re

class MySpider(Spider):
    name = 'classemlak_com'    
    start_urls = ["http://www.classemlak.com/kiralik_konutlar/"]
    execution_type='testing'
    country='turkey'
    locale='tr'
    external_source='Classemlak_PySpider_turkey_tr'
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='item']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
         

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        prop_type = response.xpath("//div[@id='page']/h1/text()").get()
        if prop_type and "DAİRE" in prop_type:
            item_loader.add_value("property_type", "apartment")
        elif prop_type and "RESİDENCE" in prop_type:
            item_loader.add_value("property_type", "apartment")
        elif prop_type and "VİLLA" in prop_type:
            item_loader.add_value("property_type", "house")
        else:
            return
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Classemlak_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//title//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = response.xpath("//td[@class='adres']/text()").get()
        if address:
            address = address.strip()
            item_loader.add_value("address", address)

        latitude = response.xpath("//div[@id='StreetView']//iframe/@src").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('!1d')[1].split('!')[0].strip())
            item_loader.add_value("longitude", latitude.split('!2d')[1].split('!')[0].strip())
        else:
            latitude = response.xpath("//div[@id='GoogleMaps']//iframe/@src").get()
            if latitude:
                item_loader.add_value("latitude", latitude.split('!3d')[1].split('!')[0].strip())
                item_loader.add_value("longitude", latitude.split('!2d')[1].split('!')[0].strip())

        if response.xpath("//b[contains(.,'FULL EŞYALI')]").get(): item_loader.add_value("furnished", True)

        square_meters = response.xpath("//td[contains(.,'Metrekare (Net)')]/following-sibling::td/text()").get()
        if square_meters:
            square_meters = square_meters.split('m')[0].strip()
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//td[contains(.,'Oda')]/text()[2]").get()
        if room_count:
            if len(room_count.split('+')) > 1:
                room_count = str(int(float(room_count.split('+')[0].strip()) + float(room_count.split('+')[1].strip())))
            else:
                room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        rent = response.xpath("//table[@id='dtable']//tr[1]/th/text()").get()
        if rent:
            rent = rent.strip()
            item_loader.add_value("rent_string", rent)

        external_id = response.xpath("//td[contains(.,'İlan No')]/following-sibling::td/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)
        
        bathroom=response.xpath("//td[contains(.,'Banyo')]/following-sibling::td/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
        
        description = " ".join(response.xpath("//h2[2]/preceding-sibling::*[preceding-sibling::h2]//text()").getall()).strip()  
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        city = response.xpath("//td[@class='adres']/text()").get()
        if city:
            city = city.split('/')[0].strip()
            item_loader.add_value("city", city)

        images = [urljoin('http://www.classemlak.com', x) for x in response.xpath("//div[@id='imageWrap']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//td[contains(.,'Depozito')]/following-sibling::td/text()").get()
        if deposit:
            deposit = deposit.strip()
            item_loader.add_value("deposit", deposit)

        floor = response.xpath("//td[contains(.,'Bulunduğu Kat')]/following-sibling::td/text()").get()
        if floor:
            floor = floor.strip()
            item_loader.add_value("floor", floor)

        parking = response.xpath("//div[@class='specs']//span[contains(.,'Otopark')]").get()
        if parking:
            parking = True
            item_loader.add_value("parking", parking)

        elevator = response.xpath("//div[@class='specs']//span[contains(.,'Asansör')]").get()
        if elevator:
            elevator = True
            item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//div[@class='specs']//span[contains(.,'Balkon')]").get()
        if balcony:
            balcony = True
            item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//div[@class='specs']//span[contains(.,'Teras')]").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)

        swimming_pool = response.xpath("//div[@class='specs']//span[contains(.,'Havuz')]").get()
        if swimming_pool:
            swimming_pool = True
            item_loader.add_value("swimming_pool", swimming_pool)

        item_loader.add_value("landlord_name", "CLASS EMLAK")
        landlord_phone = response.xpath("//a[@class='iletisimHref' and contains(@href,'tel')]/strong/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone.replace("+",""))

        landlord_email = response.xpath("//a[@class='iletisimHref']/img[contains(@src,'envelope')]/parent::a/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        
        yield item_loader.load_item()
        
       

        
        
          

        

      
     