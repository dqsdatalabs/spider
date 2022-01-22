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

class MySpider(Spider):
    name = 'realtyworld_com'
    start_urls = ['https://www.realtyworld.com.tr/tr/portfoyler?Page_No=1']
    execution_type='testing'
    country='turkey'
    locale='tr'  # LEVEL 1
    external_source="Realtyworld_PySpider_turkey_tr"

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)

        seen = False
        for item in response.xpath("//div[@class='property']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.realtyworld.com.tr/tr/portfoyler?Page_No={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        if "cafe" in response.url:
            return

        item_loader.add_value("title", response.xpath("//h1/text()").get())
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)
        factory = response.xpath("//dt[contains(.,'Gayrimenkul Tipi') ]/following-sibling::dd[1]/text()").get()
        if factory and "fabrika" in factory.lower():
            return
        
        title = response.xpath("//div[@class='htitleB mt-5']/h1/text()").get()
        if "sezonluk" in title.lower():
            return
        not_list=['satılık', 'ticari','kurumsal','ofis','arsa','tarla','parsel','arazi', 'işyeri']
        for element in not_list:
            if element in title.lower():
                return

        status = response.xpath("//dt[contains(.,'İşlem Tipi')]//following-sibling::dd[1]//text()[contains(.,'Satılık')]").get()
        if status:
            return
        else:
            status = response.xpath("//dt[contains(.,'Gayrimenkul Tipi')]//following-sibling::dd[1]//text()").get()
            if status:
                if "yazlık" in status.lower() or "otel" in status.lower() or "büfe" in status.lower() or "restoran" in status.lower() or "arsa" in status.lower() or "ofis" in status.lower() or "bina" in status.lower() or "dükkan" in status.lower() or "depo" in status.lower() or "işhanı" in status.lower():
                    return
               
        property_type = ''
        status = False
        apartment_list = ['DAİRE', 'BİNA', 'DUBLEX', 'REZİDANS', 'ARAKAT', 'APART']
        house_list = ['MÜSTAKİL', 'VİLLA', 'TAŞ EV']
        if not status:
            for element in apartment_list:
                if element in title.upper():
                    property_type = 'apartment'
                    status = True
                    break
        if not status:
            for element in house_list:
                if element in title.upper():
                    property_type = 'house'
                    status = True
                    break
                
        room_count = response.xpath("//dt[contains(.,'Oda Sayısı') or contains(.,'Bölüm Sayısı')]/following-sibling::dd[1]/text()").get()
        if room_count:
            if len(room_count.split('+')) > 1:
                if len(room_count.split('(')) > 1:
                    room_count = str(int(float(room_count.split('(')[1].split('+')[0]) + float(room_count.split('(')[1].split('+')[1].strip(')'))))
                else:
                    room_count = str(int(int(room_count.split('+')[0].strip()) + int(room_count.split('+')[1].strip())))
            else:
                room_count = room_count.strip().split(' ')[0]
            item_loader.add_value("room_count", room_count)
        
        if not status and room_count:
            if room_count.isdigit():
                if int(room_count) > 1:
                    property_type = 'apartment'
        if property_type:
            item_loader.add_value("property_type", property_type)
        else: return
        
        address = response.xpath("//div/address//text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip().split(' ')[0].strip())

        latitude_longitude = response.xpath("//script[contains(., '_latitude')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('_latitude = ')[1].split(';')[0].strip()
            longitude = latitude_longitude.split('_longitude = ')[1].split(';')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
                
        
        square_meters = response.xpath("//dt[contains(.,'Metrekare')]/following-sibling::dd[1]/text()").get()
        if square_meters:
            square_meters = square_meters.strip()
            item_loader.add_value("square_meters", square_meters)
        

        rent = response.xpath("//dt[contains(.,'Fiyat')]/following-sibling::dd[1]/text()").get()
        if rent:
            rent = rent.strip()
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "TRY")

        bathroom_count=response.xpath("//dt[contains(.,'Banyo')]/following-sibling::dd[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        external_id = response.xpath("//dt[contains(.,'İlan No')]/following-sibling::dd[1]/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//div[@class='portfoyinfo']/p/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d + ' '
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        city = response.xpath("//address/text()").get()
        if city:
            if 'İstanbul' in city:
                city = 'İstanbul'
            else:
                city = city.strip().split(' ')[-1]
            item_loader.add_value("city", city)

        images = [x for x in response.xpath("//li[@class='item']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        furnished = response.xpath("//li[contains(.,'Mobilya')]").get()
        if furnished:
            furnished = True
            item_loader.add_value("furnished", furnished)

        floor = response.xpath("//dt[contains(.,'Bulunduğu Kat')]/following-sibling::dd[1]/text()").get()
        if floor:
            floor = floor.strip()
            item_loader.add_value("floor", floor)

        parking = response.xpath("//li[contains(.,'Otopark')]").get()
        parking2 = response.xpath("//div[@class='portfoyinfo']/p/text()[contains(.,'Otopark') or contains(.,'OTOPARK')]").get()
        if parking or parking2:
            parking = True
            item_loader.add_value("parking", parking)

        elevator = response.xpath("//li[contains(.,'Asansör')]").get()
        elevator2 = response.xpath("//div[@class='portfoyinfo']/p/text()[contains(.,'Asansör') or contains(.,'ASANSÖR')]").get()
        if elevator or elevator2:
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

        washing_machine = response.xpath("//li[contains(.,'Çamaşır Makinesi')]").get()
        all_machines = response.xpath("//li[contains(.,'Beyaz Eşya')]").get()
        if washing_machine or all_machines:
            washing_machine = True
            item_loader.add_value("washing_machine", washing_machine)

        dishwasher = response.xpath("//li[contains(.,'Bulaşık Makinesi')]").get()
        all_machines = response.xpath("//li[contains(.,'Beyaz Eşya')]").get()
        if dishwasher or all_machines:
            dishwasher = True
            item_loader.add_value("dishwasher", dishwasher)

        landlord_name = response.xpath("//div[@class='agent-contact-info']/h3/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//dt[contains(.,'Telefon:')]/following-sibling::dd[1]/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            landlord_phone = response.xpath("//dt[contains(.,'Mobil:')]/following-sibling::dd[1]/text()").get()
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//dt[contains(.,'E-posta:')]/following-sibling::dd[1]/a/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        
        status = response.xpath("//h1/text()[contains(.,'SATILIK')]").get()
        if not status:
            yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data