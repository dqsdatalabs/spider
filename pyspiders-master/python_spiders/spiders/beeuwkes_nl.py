# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re


class MySpider(Spider):
    name = 'beeuwkes_nl'
    start_urls = ['https://www.beeuwkes.nl/aanbod/huur?page=1']  # LEVEL 1
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source='Beeuwkes_PySpider_netherlands_nl'
    # 1. FOLLOWING
    def parse(self, response):
        
        for follow_url in response.xpath("//div[contains(@class,'row view view-tiles')]/div[contains(@class,'object_wrapper')]/a/@href").extract():
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[@rel='next']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Beeuwkes_PySpider_" + self.country + "_" + self.locale)

        
        #{'appartement': 'OK', 'woonhuis': 'OK', None: 'OK'}
        property_type = response.xpath("//dt[.='Soort woning']/following-sibling::dd[1]/text()").get()
        if property_type and "appartement" in property_type:
            item_loader.add_value("property_type", "apartment")
        elif property_type and "woonhuis" in property_type:
            item_loader.add_value("property_type", "house")
        else:
            return
        rented = response.xpath("//div[@class='container-fluid']//dl/dt[.='Status']/following-sibling::dd[1]//text()[contains(.,'verhuurd')]").get()
        if rented:
            return
        
        title = response.xpath("//div[@class='col-xs-12 col-md-8']/h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)

        external_id = (response.url).split('-')[-1]
        item_loader.add_value("external_id", external_id)

        desc = "".join(response.xpath("//p[@class='description_long']/text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc.strip())

        address = response.xpath("//meta[@name='keywords']/@content").get()
        if address:
            item_loader.add_value("address", address.split(',')[0].strip() + ', ' +address.split(',')[1].strip())
            item_loader.add_value("city", address.split(',')[1].strip())
        
        zipcode = response.xpath("substring-before(substring-after(//iframe/@src[contains(.,'map')],','),',')").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        floor = response.xpath("//dt[.='Aantal woonlagen']/following-sibling::dd[1]/text()").get()
        if floor:
            floor = floor.strip()
            item_loader.add_value("floor", floor)       
        
        square_meters = response.xpath("//dt[.='Woonoppervlakte']/following-sibling::dd[1]/text()").get()
        if square_meters:
            square_meters = square_meters.split(' ')[0]
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//dt[.='Aantal slaapkamers']/following-sibling::dd[1]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//dt[.='Aantal kamers']/following-sibling::dd[1]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
            
        price = response.xpath("//div[@class='col-xs-12 col-md-4 text-md-right']/h2/text()[not(contains(.,'ver'))]").get()
        if price: 
            item_loader.add_value("rent_string", price)
        
        images = [x for x in response.xpath("//div[@id='fotos']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        energy_label = response.xpath("//dt[.='Energielabel']/following-sibling::dd[1]/text()").get()
        if energy_label:
            energy_label = energy_label.strip()
            item_loader.add_value("energy_label", energy_label)
        
        item_loader.add_value("landlord_name", "Beeuwkes Makelaardij & Consultancy")
        item_loader.add_value("landlord_phone", "070-3143000")
        item_loader.add_value("landlord_email", "info@beeuwkes.nl")

        elevator = response.xpath("//dt[.='Voorzieningen']/following-sibling::dd[1]/text()[contains(.,'lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        furnished = response.xpath("//dt[.='Bijzonderheden']/following-sibling::dd[1]/text()").get()
        if furnished:
            if "gemeubileerd" in furnished:
                item_loader.add_value("furnished", True)

        parking = response.xpath("//dt[.='Bijzonderheden']/following-sibling::dd[1]/text()").get()
        if parking:
                item_loader.add_value("parking", True)
        yield item_loader.load_item()