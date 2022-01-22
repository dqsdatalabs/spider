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
    name = 'elvi_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    thousand_separator = ','
    scale_separator = '.' # LEVEL 1

    def start_requests(self):
        start_urls = [
            {"url": "https://elvi.fr/rechercher?category=2&type=5&page=1",
            "prop_type": "apartment"},
            {"url": "https://elvi.fr/rechercher?category=2&type=6&page=1",
            "prop_type": "apartment"},    
            {"url": "https://elvi.fr/rechercher?category=2&type=18&page=1",
            "prop_type": "house"},
            {"url": "https://elvi.fr/rechercher?category=2&type=13&page=1",
            "prop_type": "house"},
            {"url": "https://elvi.fr/rechercher?category=2&type=19&page=1",
            "prop_type": "house"},
            {"url": "https://elvi.fr/rechercher?category=2&type=14&page=1",
            "prop_type": "house"},     
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={"type":url.get('type'), "property_type":url.get('prop_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@id='listing']//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = ''
            if 'category=2&type=5' in response.url:
                url = f"https://elvi.fr/rechercher?category=2&type=5&page={page}"
            elif 'type=6' in response.url:
                url = f"https://elvi.fr/rechercher?category=2&type=6&page={page}"
            elif 'type=18' in response.url:
                url = f"https://elvi.fr/rechercher?category=2&type=18&page={page}"
            elif 'type=13' in response.url:
                url = f"https://elvi.fr/rechercher?category=2&type=13&page={page}"
            elif 'type=19' in response.url:
                url = f"https://elvi.fr/rechercher?category=2&type=19&page={page}"
            elif 'type=14' in response.url:
                url = f"https://elvi.fr/rechercher?category=2&type=14&page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        property_type = response.meta.get("property_type")
        item_loader.add_value("property_type", property_type)

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Elvi_PySpider_"+ self.country + "_" + self.locale)

        title = "".join(response.xpath("//head/title//text()").getall())
        if title:
            item_loader.add_value("title", title)

            if "meuble" in title.lower():
                item_loader.add_value("furnished", True)

        address = response.xpath("//div[@class='presenter row']/div/text()").get()
        if address:
            address = address.strip()
            item_loader.add_value("address", address)

        square_meters = response.xpath("//span[contains(.,'Surface')]/following-sibling::text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('m')[0].strip())))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//span[contains(.,'Chambre')]/following-sibling::text()").get()
        if room_count:
            room_count = room_count.strip().split(' ')[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//span[contains(.,'Pièces')]/following-sibling::text()").get()
            if room_count:
                room_count = room_count.strip().split(' ')[0]
                item_loader.add_value("room_count", room_count)

        rent = response.xpath("//div[@class='price']/span/text()").get()
        if rent:
            rent = rent.strip()
            item_loader.add_value("rent_string", rent)

        external_id = response.url.split('-')[-1]
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//h3[.='Description']/following-sibling::p/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        if address:
            city = address.split('(')[0].strip()
            item_loader.add_value("city", city)

        if address:
            zipcode = address.split('(')[1].split(')')[0].strip()
            item_loader.add_value("zipcode", zipcode)

        images = [x for x in response.xpath("//div[@class='col-xs-12 gallery']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//br[contains(following-sibling::text(), 'DEPOT')]/following-sibling::text()[1]").get()
        if deposit:
            deposit = deposit.split(':')[1].replace('€', '').strip().replace(' ', '').replace(',', '').replace('.', '')
            item_loader.add_value("deposit", deposit)

        energy_label = response.xpath("//div[@class='dpe']/div[@class='letter']/text()").get()
        if energy_label:
            energy_label = energy_label.strip()
            item_loader.add_value("energy_label", energy_label)

        parking = response.xpath("//span[contains(.,'Parking')]/following-sibling::text()").get()
        if parking:
            parking = True
            item_loader.add_value("parking", parking)

        elevator = response.xpath("//span[contains(.,'Ascenseur')]/following-sibling::text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                elevator = True
            elif elevator.strip().lower() == 'non':
                elevator = False
            if type(elevator) == bool:
                item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//span[contains(.,'Balcon')]/following-sibling::text()").get()
        if balcony:
            balcony = True
            item_loader.add_value("balcony", balcony)

        landlord_name = response.xpath("//h3[@class='agency-name']/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//div[@class='agency-phone']/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)


        yield Request(
            url=response.xpath("//iframe[contains(@class,'hide-on-print')]/@src").get(),
            callback=self.get_latlng,
            dont_filter=True,
            meta={
                "item":item_loader,
            }
        )

    def get_latlng(self, response):
        item_loader = response.meta["item"]

        script_data = response.xpath("//script[contains(.,'onApiLoad()')]/text()").get()
        if script_data:
            latlng = script_data.split('",[')[1].split(']],"')[0].strip()
            lat = latlng.split(",")[0].strip()
            lng = latlng.split(",")[1].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data