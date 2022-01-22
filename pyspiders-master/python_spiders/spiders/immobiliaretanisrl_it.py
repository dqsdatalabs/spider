# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'immobiliaretanisrl_it'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = 'Immobiliaretanisrl_PySpider_italy'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.immobiliaretanisrl.it/cerca-sulla-mappa/?codice-immobile=&advanced_city=&advanced_area=&filter_search_action%5B%5D=affitto&filter_search_type%5B%5D=residenziale&superficie-min-in-mq=&prezzo-min-in-e=&prezzo-max-in-e=&submit=CERCA",
                ],
                "property_type" : "apartment"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='listing_ajax_container']//h4/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//h1[@class='entry-title entry-prop']/text()").get()
        if title:
            item_loader.add_value("title", title)

        rent = response.xpath("(//div[@class='price_area']/text())[1]").get()
        if rent:
            item_loader.add_value("rent", rent.split('€')[0].replace(".","").strip())
        item_loader.add_value("currency", "EUR")
        
        utilities = response.xpath("//div[@class='listing_detail col-md-4'][contains(.,'Spese')]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('Spese:')[-1].replace(".","").split('mensili')[0].strip())
        
        square_meters = response.xpath("//div[@class='listing_detail col-md-4'][contains(.,'Superficie')]/strong/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('mq')[0].strip())

        room_count = response.xpath("//div[@class='listing_detail col-md-4'][contains(.,'Numero locali')]/strong/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
            if room_count and ">" in room_count:
                item_loader.add_value("room_count", room_count.split('>')[-1].strip())

        bathroom_count = response.xpath("//div[@class='listing_detail col-md-4'][contains(.,'bagni')]/strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        city = response.xpath("//div[@class='listing_detail']/strong[2]/a/text()").get()
        if city:
            item_loader.add_value("city", city)
        zipcode = response.xpath("(//div[@class='listing_detail']/text())[2]").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.replace(",","").strip())

        address = "".join(response.xpath("//div[@class='listing_detail']/text()").getall())
        if address:
            address = address.split('via')[-1].strip()
            add = response.xpath("//div[@class='listing_detail']/strong[1]/a/text()").get()
            item_loader.add_value("address", address+" "+add)

        floor = response.xpath("//div[@class='listing_detail col-md-4'][contains(.,'piano')]/strong/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        energy_label = response.xpath("//div[@class='listing_detail col-md-4'][contains(.,'Classe Energetica')]/img/@src").get()
        if energy_label:
            energy_label = energy_label.split('energetiche/')[-1].split('.')[0].strip()
            item_loader.add_value("energy_label", energy_label.upper())

        desc = "".join(response.xpath("//div[@class='single-content listing-content']/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        furnished = response.xpath("//div[@class='listing_detail col-md-4'][contains(.,'Arredamento')]/strong/text()").get()
        if furnished and "non" not in furnished.lower():
            item_loader.add_value("furnished", True)

        parking = response.xpath("//div[@class='listing_detail col-md-4'][contains(.,'Box auto')]/strong/text()").get()
        if parking and "assente" not in parking.lower():
            item_loader.add_value("parking", True)

        elevator = response.xpath("//div[@class='listing_detail col-md-4']/text()[contains(.,'Ascensore')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        latitude = response.xpath("//div/@data-cur_lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        longitude = response.xpath("//div/@data-cur_long").get()
        if longitude:
            item_loader.add_value("longitude", longitude)

        images = [x for x in response.xpath("//div[@class='carousel-inner']/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        landlord_name = response.xpath("//div[@class='col-md-8 agent_details']/h3/a/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        landlord_email = response.xpath("//div[@class='agent_detail agent_email_class']/a/text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        landlord_phone = response.xpath("(//div[@class='agent_detail']/a/text())[1]").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()