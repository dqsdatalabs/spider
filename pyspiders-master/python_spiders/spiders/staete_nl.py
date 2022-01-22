# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader 
import json
import re


class MySpider(Spider):
    name = 'staete_nl'
    start_urls = ['https://www.staete.nl/aanbod/?Huur=1'] 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'js-residences-container')]/div[contains(@class,'layout__cell--card')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)


        rented = response.xpath("//div[@class='object-header__title']/span[.='Verhuurd']/text()").extract_first()
        if rented:
            return

        item_loader.add_value("external_source", "Staete_PySpider_" + self.country + "_" + self.locale)

        
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        
        desc = "".join(response.xpath("//div[@class='js-tab-content tabs__content is--active']/text()").extract())
        if desc:
            description = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", description.strip())
        if "terras" in desc:
            item_loader.add_value("terrace", True)

        prop = response.xpath("//dl[contains(@class,'definition-list')]/dt[contains(.,'Soort woning')]/following-sibling::dd[1]/text()").get()
        if prop:
            if "Bovenwoning" in prop :
                property_type = "apartment"
                item_loader.add_value("property_type", property_type)
            if  "Galerijflat" in prop  :
                item_loader.add_value("property_type", "house")

            if  "Eengezinswoning" in prop or "Tussenwoning" in prop :
                item_loader.add_value("property_type", "house")
                
            if "Benedenwoning" in prop or "Portiekflat" in prop:
                property_type = "apartment"
                item_loader.add_value("property_type", property_type)
        elif response.xpath("//dd[contains(.,'Appartement')]/text()").get():
            item_loader.add_value("property_type", "apartment")
        elif 'appartement' in desc.lower():
            item_loader.add_value("property_type", "apartment")
        else:
             prop = response.xpath("//div[contains(@class,'island')]//ul/li[contains(.,'Type woningen')]").get()
             if prop:
                property_type = "house"
                item_loader.add_value("property_type", property_type)

            

        price = response.xpath("//div[@class='object-header__price']/text()").extract_first()
        if "-" in price:
            item_loader.add_value("rent", price.split("-")[0].split("€")[1])
        else:
            item_loader.add_value("rent_string", price)
        item_loader.add_value("currency", "EUR")

        square = response.xpath("//dl[@class='definition-list definition-list--closed']/dt[.='Woonoppervlakte']/following-sibling::dd[1]/text()").get()
        if square:
            item_loader.add_value("square_meters", square.split("m")[0])

        room_count = "".join(response.xpath("//div[@class='js-tab-content tabs__content is--active']/text()[contains(.,' slaapkamer')][last()]").getall())
        if room_count:
            try:
                room = room_count.split("van")[1].split("slaapkamer")[0]
                item_loader.add_value("room_count", room.strip())
            except: pass

        images = [response.urljoin(x)for x in response.xpath("//div[@class='media-gallery']//a/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_xpath("room_count","normalize-space(//dl[@class='definition-list definition-list--closed']/dt[.='Aantal kamers']/following-sibling::dd[1]/text())")
        item_loader.add_xpath("floor","normalize-space(//dl[@class='definition-list definition-list--closed']/dt[.='Aantal woonlagen']/following-sibling::dd[1]/text())")

        floor_plan_images=response.xpath("//a[contains(@data-fancybox,'floor')]/@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", response.urljoin(floor_plan_images))
            
        terrace = "".join(response.xpath("normalize-space(//dl[@class='definition-list definition-list--closed']/dt[.='Garage']/following-sibling::dd[1]/text())").extract()).strip()
        if terrace:
            item_loader.add_value("parking", True)

        address = "".join(response.xpath("//h1[@class='object-header__heading']/span/text()").extract())

        item_loader.add_value("zipcode", address.split(" ")[0])
        item_loader.add_value("address",address)
        item_loader.add_value("city", address.split(" ")[-1])

        latlng = "".join(response.xpath("//main/script[contains(.,'const mapLocations')]/text()").extract())
        if latlng:
            item_loader.add_value("latitude", latlng.split("lat:")[1].split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split("lng:")[1].split("}")[0].strip())


        item_loader.add_value("landlord_phone", "0416 - 344 944")
        item_loader.add_value("landlord_email", "info@staete.nl")
        item_loader.add_xpath("landlord_name", "//div[@class='heading-xs text-center']/text()")


        yield item_loader.load_item()