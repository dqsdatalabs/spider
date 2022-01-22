# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from python_spiders.loaders import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = "ooms_com"
    start_urls = [
        "https://ooms.com/api/properties/available.json"
    ] 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        
        jresp = json.loads(response.body)
        for item in jresp['objects']:
            if item.get('buy_or_rent') != "rent":
                continue
            
            item_loader = ListingLoader(response=response)
            
            follow_url = response.urljoin(item.get('url'))
            prop = item.get('house_type')
            if prop:
                if "Appartement" in prop:
                    prop = "apartment"
                elif "Woonhuis" in prop:
                    prop = "house"
                    
                # gerekenler burada dolabilir https://prnt.sc/uskzjd
                item_loader.add_value("title", item.get('title'))
                item_loader.add_value("external_link", follow_url)
                item_loader.add_value("rent", str(item.get('rent_price')))
                item_loader.add_value("currency", "EUR")
                item_loader.add_value("property_type", prop)
                item_loader.add_value("address", item.get('street_name'))
                #item_loader.add_value("room_count", str(item.get('amount_of_rooms')))
                item_loader.add_value("square_meters", str(item.get('usable_area_living_function')))
                item_loader.add_value("latitude", str(item.get('lat')))
                item_loader.add_value("longitude", str(item.get('lng')))
                
                item_loader.add_value("city", item.get('place'))
                item_loader.add_value("zipcode", str(item.get('zip_code')))


                yield response.follow(follow_url, callback=self.populate_item, meta={'item': item_loader})

    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):
        item_loader = response.meta.get("item")

        item_loader.add_value("external_source", "Ooms_PySpider_" + self.country + "_" + self.locale)

        item_loader.add_xpath("floor", "normalize-space(//tr[th[. ='Aantal woonlagen']]/td/text())")

        room_count = response.xpath("//th[contains(.,'Aantal slaapkamers')]/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(' ')[0].strip())

        bathroom_count = response.xpath("//th[contains(.,'Aantal badkamers')]/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(' ')[0].strip())

        utilities = response.xpath("normalize-space(//tr[th[. ='Servicekosten']]/td/text())").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[1])

        images = [response.urljoin(x)for x in response.xpath("//div[@class='grid-media grid-media--all-images']//picture//@data-src").extract()]
        if images:
                item_loader.add_value("images", images)

        desc = "".join(response.xpath("//div[@class='panel__block tabs-description']/p/text()").extract())
        item_loader.add_value("description", desc)

        available_date = "".join(response.xpath("normalize-space(//tr[th[. ='Aanvaarding']]/td/text())").extract())
        if "in overleg" not in available_date:
            item_loader.add_value("available_date", available_date)

        terrace = response.xpath("//tr[th[. ='Voorzieningen']]/td/ul/li/text()[contains(.,'lift')]").get()
        if terrace:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//tr[th[. ='Garage']]/td/ul/li/text()[. !='geen garage']").get()
        if terrace:
            item_loader.add_value("parking", True)

        phone = response.xpath("//ul[@class='contact-us-list']/li/a/@href[contains(.,'tel:')]").extract_first()
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("tel:",""))
        landlord_email = response.xpath("//ul[@class='contact-us-list']/li/a/@href[contains(.,'mailto:')]").extract_first()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.replace("mailto:",""))
        item_loader.add_value("landlord_name", "Rotterdam")

        yield item_loader.load_item()
