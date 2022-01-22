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
    name = 'emlakmarktatasehir_com'
    execution_type='testing'
    country='turkey'
    locale='tr'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://atasehiremlakmarkt.sahibinden.com/kiralik-daire?sorting=storeShowcase",
                "property_type" : "apartment"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='description']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        
        next_page = response.xpath("//a[@class='prevNextBut']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.xpath("//div[@class='classifiedDetailTitle']/h1/text()").get()
        item_loader.add_value("title", title)

        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Emlakmarktatasehir_PySpider_"+ self.country + "_" + self.locale)

        latitude = response.xpath("//div[@id='gmap']/@data-lat").get()
        longitude = response.xpath("//div[@id='gmap']/@data-lon").get()
        if latitude and longitude:
            latitude = latitude.strip()
            longitude = longitude.strip()
           
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        address2="".join(response.xpath("//div[contains(@class,'Info')]/h2//text()").getall())
        if address2:
            city=address2.split("/")[0].strip()
            address = re.sub("\s{2,}", " ", address2.strip())
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city)
            
        square_meters = response.xpath("//strong[contains(.,'m² (Net)')]/parent::li/span/text()").get()
        if square_meters:
            square_meters = square_meters.strip()
        else:
            square_meters = response.xpath("//strong[contains(.,'m²')]/parent::li/span/text()").get()
            if square_meters:
                square_meters = square_meters.strip()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//strong[contains(.,'Oda Sayısı')]/parent::li/span/text()").get()
        if room_count:
            if len(room_count.split('+')) > 1:
                if len(room_count.split('(')) > 1:
                    room_count = str(int(float(room_count.split('(')[1].split('+')[0]) + float(room_count.split('(')[1].split('+')[1].strip(')'))))
                else:
                    room_count = str(int(float(room_count.split('+')[0].strip()) + float(room_count.split('+')[1].strip())))
            else:
                room_count = room_count.strip().split(' ')[0]
            item_loader.add_value("room_count", room_count)
            
        if "stüdyo" in room_count.lower():
            item_loader.add_value("property_type", "studio")
            
        bathroom=response.xpath("//strong[contains(.,'Banyo')]/parent::li/span/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.strip())
        
        rent = response.xpath("///div[contains(@class,'Info')]/h3/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(".",""))

        external_id = response.xpath("//strong[contains(.,'İlan No')]/parent::li/span/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//div[@id='classifiedDescription']//text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d + ' '
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        city = response.xpath("//div[@class='classified-criterias']/p[@class='location']/text()").get()
        if city:
            city = city.strip().split('/')[0].strip()
            item_loader.add_value("city", city)

        images = [x for x in response.xpath("//ul[@class='classifiedDetailThumbList ']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
      
        deposit = response.xpath("//strong[contains(.,'Depozito')]/parent::li/span/text()").get()
        if deposit:
            deposit = deposit.strip()
            if deposit.isnumeric():
                item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//strong[contains(.,'Aidat')]/parent::li/span/text()").get()
        if utilities:
            utilities = utilities.strip()
            if utilities.isnumeric():
                item_loader.add_value("utilities", utilities)

        furnished = response.xpath("//strong[contains(.,'Eşyalı')]/parent::li/span/text()").get()
        if furnished:
            if furnished.strip().lower() == 'evet' or furnished.strip().lower() == 'var':
                furnished = True
            else:
                furnished = False
            item_loader.add_value("furnished", furnished)

        floor = response.xpath("//strong[contains(.,'Bulunduğu Kat')]/parent::li/span/text()").get()
        if floor:
            floor = floor.strip()
            item_loader.add_value("floor", floor)

        parking = response.xpath("//li[@class='selected' and contains(.,'Otopark')]").get()
        if parking:
            parking = True
            item_loader.add_value("parking", parking)

        elevator = response.xpath("//li[@class='selected' and contains(.,'Asansör')]").get()
        if elevator:
            elevator = True
            item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//strong[contains(.,'Balkon')]/parent::li/span/text()").get()
        if balcony:
            if balcony.strip().lower() == 'evet' or balcony.strip().lower() == 'var':
                balcony = True
            else:
                balcony = False
            item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//li[@class='selected' and contains(.,'Teras')]").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)

        swimming_pool = response.xpath("//li[@class='selected' and contains(.,'Havuz')]").get()
        if swimming_pool:
            swimming_pool = True
            item_loader.add_value("swimming_pool", swimming_pool)

        washing_machine = response.xpath("//li[@class='selected' and contains(.,'Çamaşır Makinesi')]").get()
        all_machines = response.xpath("//li[@class='selected' and contains(.,'Beyaz Eşya')]").get()
        if washing_machine or all_machines:
            washing_machine = True
            item_loader.add_value("washing_machine", washing_machine)

        dishwasher = response.xpath("//li[@class='selected' and contains(.,'Bulaşık Makinesi')]").get()
        all_machines = response.xpath("//li[@class='selected' and contains(.,'Beyaz Eşya')]").get()
        if dishwasher or all_machines:
            dishwasher = True
            item_loader.add_value("dishwasher", dishwasher)

        landlord_name = response.xpath("//div[@class='username-info-area']//h5/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//ul[@id='phoneInfoPart']/li[2]/span[1]/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.replace("(","").replace(")",""))

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data