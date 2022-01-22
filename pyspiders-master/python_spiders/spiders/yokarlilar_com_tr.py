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
    name = 'yokarlilar_com_tr'
    start_urls = ['http://www.yokarlilar.com.tr/index.php?option=com_hotproperty&view=type&id=5&Itemid=30']  # LEVEL 1
    execution_type='testing'
    country='turkey'
    locale='tr'

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 10)
        
        seen = False
        for item in response.xpath("//a[@class='hp_title']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        if "id=5" in response.url:
            if page == 10 or seen:
                url = f"http://www.yokarlilar.com.tr/index.php?option=com_hotproperty&view=type&id=5&Itemid=30&limitstart={page}"
                yield Request(url, callback=self.parse, meta={"page": page+10})
        elif "id=6" in response.url:
            if page == 10 or seen:
                url = f"http://www.yokarlilar.com.tr/index.php?option=com_hotproperty&view=type&id=6&Itemid=30&limitstart={page}"
                yield Request(url, callback=self.parse, meta={"page": page+10})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        sale = response.xpath("//span[contains(.,'Tür')]/following-sibling::text()[1]").get()
        if sale and "satılık" in sale.lower():
            return

        holiday = " ".join(response.xpath("//div[@id='heading_Prop']/text()").extract())
        if "sezonluk" in holiday.lower():
            return

        title = "".join(response.xpath("//div[@id='heading_Prop']/text()").extract())
        item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Yokarlilar_PySpider_"+ self.country + "_" + self.locale)

        address =", ".join(response.xpath("//div[@id='hp_view_addr']/text()").getall())
        if address:
            item_loader.add_value("address",address.upper().strip())
        
        item_loader.add_value("property_type", 'house')

        square_meters = response.xpath("//span[contains(.,'Alan')]/following-sibling::text()[1]").get()
        if square_meters:
            square_meters = square_meters.strip()
            item_loader.add_value("square_meters", square_meters)
        
        bath_room = response.xpath("//span[contains(.,'Banyo Sayısı')]/following-sibling::text()[1]").get()
        if bath_room:
            item_loader.add_value("bathroom_count", bath_room)

        room_count = response.xpath("//span[contains(.,'Oda')]/following-sibling::text()[1]").get()
        saloon_count = response.xpath("//span[contains(.,'Salon')]/following-sibling::text()[1]").get()
        total_room_count = None
        if room_count and saloon_count:
            total_room_count = str(int(float(room_count.strip()) + float(saloon_count.strip())))
        else:
            if room_count:
                total_room_count = room_count.strip()
            if saloon_count:
                total_room_count = saloon_count.strip()
        if total_room_count:
            item_loader.add_value("room_count", total_room_count)
        

        rent = response.xpath("//span[contains(.,'Fiyat')]/following-sibling::span[1]/text()[. !='0']").get()
        if rent:
            rent = rent.strip()
            item_loader.add_value("rent", rent)
        currency = 'TRY'
        item_loader.add_value("currency", currency)

        utilities = response.xpath("//p[contains(.,'Aidat')]//text()[.!='0']").get()
        if utilities:
            if utilities.isdigit():
                utilities = utilities.split(":")[1].split("TL")[0]
                item_loader.add_value("utilities", utilities.strip())

        if "ID" in title or " ıd " in title:
            external_id = title.upper().split('ID')[1].strip()
            item_loader.add_value("external_id", external_id)
        
        description =" ".join(response.xpath("//div[@class='hp_view_details']/p//text()").getall())    
        if description:
            item_loader.add_value("description", description.strip())
            if "eşyasız" in description.lower():
                item_loader.add_value("furnished",False)
            if "eşyalı" in description.lower():
                item_loader.add_value("furnished",True)

        city = response.xpath("//div[@id='hp_view_addr']/text()[2]").get()
        if city:
            city1 = city.strip().split(',')[0]
            if "BODRUM" in city1.upper():
                city1 = city.strip().split(',')[1]
            item_loader.add_value("city", city1.strip())

        zipcode = response.xpath("//div[@id='hp_view_addr']/text()[2]").get()
        if zipcode:
            if len(zipcode.strip().split(',')) > 2:
                zipcode = zipcode.strip().split(',')[2]
                item_loader.add_value("zipcode", zipcode.strip())

        deposit = response.xpath("//strong[contains(.,'Depozito')]/text()").get()
        if deposit:
            deposit = deposit.split(':')[1].split('TL')[0].strip()
        else:
            deposit = response.xpath("//p[contains(.,'Depozito')]/text()[not(contains(.,'1 Kira Bedeli Depozito.'))]").get()
            if deposit:
                deposit = deposit.split(':')[1].split('TL')[0].strip()
        if deposit:
            item_loader.add_value("deposit", deposit)

        furnished = response.xpath("//span[contains(.,'Eşyalı')]/following-sibling::text()[1]").get()
        if furnished:
            if 'var' in furnished.strip().lower():
                furnished = True
                item_loader.add_value("furnished", furnished)

        balcony = response.xpath("//span[contains(.,'Balkon')]/following-sibling::text()[1]").get()
        if balcony:
            if 'var' in balcony.strip().lower():
                balcony = True
                item_loader.add_value("balcony", balcony)

        swimming_pool = response.xpath("//span[contains(.,'Yüzme Havuzu')]/following-sibling::text()[1]").get()
        if swimming_pool:
            if 'var' in swimming_pool.strip().lower():
                swimming_pool = True
            item_loader.add_value("swimming_pool", swimming_pool)

        parking = response.xpath("//span[contains(.,'Otopark:')]/following-sibling::text()[1]").get()
        if parking:
            if 'var' in parking.strip().lower():
                parking = True
            item_loader.add_value("parking", parking)

        all_machines = response.xpath("//span[contains(.,'Beyaz Eşya')]/following-sibling::text()[1]").get()
        if all_machines:
            if 'var' in all_machines.strip().lower():
                item_loader.add_value("washing_machine", True)
                item_loader.add_value("dishwasher", True)

        landlord_name = response.xpath("//a[@id='hp_caption_agentname']/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone","90 252 385 35 36")
        item_loader.add_value("landlord_email","info@yokarlilar.com.tr")
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data