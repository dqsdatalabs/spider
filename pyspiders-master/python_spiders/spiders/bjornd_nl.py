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
from datetime import datetime
import re

class MySpider(Spider):
    name = 'bjornd_nl'
    start_urls = ['https://www.bjornd.nl/nl/realtime-listings/consumer']  # LEVEL 1
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source='Bjorndmakelaardij_PySpider_netherlands_nl'
    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        
        for item in data:
            if item["isRentals"]:
                property_type = item.get('mainType')
                if property_type in ["apartment","house"]:
                    follow_url = response.urljoin(item["url"])
                    lat = item["lat"]
                    lng = item["lng"]
                    yield Request(follow_url, callback=self.populate_item,meta={"lat":lat,"lng":lng, "property_type":property_type})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Bjorndmakelaardij_PySpider_" + self.country + "_" + self.locale)

        verhuurd = response.xpath("//dl[@class='full-details']/dt[.='Status']/following-sibling::dd[1]/text()").extract_first()
        if "Verhuurd"  not in verhuurd:

            lat = response.meta.get("lat")
            lng = response.meta.get("lng")

            property_type = response.meta.get('property_type')
            if property_type:
                item_loader.add_value("property_type", property_type)
            else:
                return
            
            title = response.xpath("//h1//text()").get()
            if title:
                title = re.sub('\s{2,}', ' ', title.strip())
                item_loader.add_value("title", title)
            item_loader.add_value("external_link", response.url)
            price = response.xpath("//dl[@class='full-details']/dt[.='Huurprijs' or .='Prijs' or .='Vraagprijs']/following-sibling::dd[1]/text()").extract_first()
            if price:
                if "—" in price:
                    price = price.split("€")[2].strip().split(" ")[0].strip()
                    item_loader.add_value("rent", price)
                else:
                    item_loader.add_value("rent", price.split("€")[1].strip().split(" ")[0].strip())
            item_loader.add_value("currency", "EUR")

            utilities = response.xpath("normalize-space(//dl[@class='full-details']/dt[.='Servicekosten']/following-sibling::dd[1]/text())").extract_first()
            if utilities:
                item_loader.add_value("utilities", utilities.split("€")[1])

            square = response.xpath("normalize-space(//dl[@class='full-details']/dt[.='Woonoppervlakte']/following-sibling::dd[1]/text())").get()
            if square:
                item_loader.add_value("square_meters", square.split("m²")[0])
                
            images = [response.urljoin(x)for x in response.xpath("//div[@class='swiper-wrapper']/div//img/@src").extract()]
            if images:
                    item_loader.add_value("images", images)

            floor_plan_images = [response.urljoin(x)for x in response.xpath("//div[@class='swiper-slide responsive-image']/img/@src").extract()]
            if floor_plan_images:
                    item_loader.add_value("floor_plan_images", floor_plan_images)

            item_loader.add_xpath("floor", "//dl[@class='full-details']/dt[.='Aantal verdiepingen']/following-sibling::dd[1]/text()")

            item_loader.add_xpath("room_count","normalize-space(//dl[@class='full-details']/dt[.='Aantal kamers']/following-sibling::dd[1]/text())")
            item_loader.add_xpath("energy_label", "//dl[@class='full-details']/dt[.='Energielabel']/following-sibling::dd[1]/text()")

            desc = "".join(response.xpath("//div[contains(@class,'expand-content')]/p/text()").extract())
            item_loader.add_value("description", desc)

            if "gemeubileerd" in desc.lower():
                item_loader.add_value("furnished", True)

            terrace = "".join(response.xpath("//dl[@class='full-details']/dt[.='Soort garage']/following-sibling::dd[1]/text() | //dl[@class='full-details']/dt[.='Soort']/following-sibling::dd[1]/text()").extract())
            if terrace:
                item_loader.add_value("parking", True)

            available_date=response.xpath("//dl[@class='full-details']/dt[.='Oplevering']/following-sibling::dd[1]/text()").get()
            if available_date:
                if available_date.lower() == "per direct":    
                    item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
                elif not available_date.replace(" ","").isalpha():
                    date_parsed = dateparser.parse(available_date)
                    date3 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date3)

            terrace = "".join(response.xpath("//dl[@class='full-details']/dt[.='Balkon']/following-sibling::dd[1]/text()").extract())
            if terrace:
                if  "Ja" in terrace or "Oui" in terrace or "Yes" in terrace:
                    item_loader.add_value("balcony", True)


            address = response.xpath("//div[contains(@class,'col-sm-10')]/p/text()").extract_first()
            item_loader.add_value("address", address.split("(")[0])
            item_loader.add_value("zipcode",address.split(",")[0])
            # item_loader.add_value("zipcode", split_address(address, "zip"))
            item_loader.add_value("city", address.split(",")[1].split("(")[0])
            item_loader.add_value("latitude", str(lat))
            item_loader.add_xpath("longitude", str(lng))


            item_loader.add_value("landlord_phone", "015 213 51 39")
            item_loader.add_value("landlord_email", "info@bjornd.nl")
            item_loader.add_value("landlord_name", "Caroline Steijger")
            
            yield item_loader.load_item()

