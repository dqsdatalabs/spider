# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = '24homerentals_nl'
    start_urls = ["https://www.24homerentals.nl/huren/page:1"] #LEVEL1

    execution_type='testing'
    country='netherlands'
    locale='nl'
    external_source = "24homerentals_PySpider_netherlands_nl"
    def parse(self, response):
        
        for item in response.xpath("//div[@class='row house-item']"):
            follow_url = response.urljoin(item.xpath(".//img[@class='img-responsive']/../@href").get())
            prop_type = item.xpath(".//h3/a/text()").get()
            if "Appartement" in prop_type or "Benedenwoning" in prop_type or "Bovenwoning" in prop_type or "Maisonnette" in prop_type:
                prop_type = "apartment"
            elif "Tussenwoning" in prop_type or "Hoekhuis" in prop_type or "gezinswoning" in prop_type:
                prop_type = "house"
            elif "Studio" in prop_type:
                prop_type = "studio"
            else:
                prop_type = None
            
            
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': prop_type})

        
        next_page = response.xpath("//a[@aria-label='Next']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", self.external_source)
        prop_type = response.meta.get('property_type')
        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else:
            return

        rented = response.xpath("//span[text()='Verhuurd']").get()
        if rented:
            return

        # rented = response.xpath("//i[@class='fa fa-eur']/following-sibling::text()").get()
        # if rented:
        #     if "Verhuurd" in rented:
        #         return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        item_loader.add_xpath("title", "//h1[@class='pull-left']/text()")

        price = "".join(response.xpath("//dl/dt[contains(.,'Huurprijs')]/following-sibling::dd[1]/text()[contains(.,'€')]").extract())
        if price:
            item_loader.add_value("rent_string", price)
        deposit = "".join(response.xpath("//dl/dt[contains(.,'Borg')]/following-sibling::dd/text()[contains(.,'€')]").extract())
        if deposit:
            item_loader.add_value("deposit", deposit.replace("€","").strip())
        meters = "".join(response.xpath("//dl/dt[contains(.,'Huurprijs')]/following-sibling::dd/text()[contains(.,'m²')]").extract())
        if meters:
            item_loader.add_value("square_meters", meters.split("m²")[0].strip())

        room = "".join(response.xpath("//dl/dt[contains(.,'Slaapkamers')]/following-sibling::dd/text()[not(contains(.,'0'))]").extract())
        if room:
            item_loader.add_value("room_count", room.strip())
        if "studio" in prop_type:
            item_loader.add_value("room_count", "1")
        bath_room = "".join(response.xpath("//ul/li[contains(.,'badkamer')]/text()[not(contains(.,'0'))]").extract())
        if bath_room:
            item_loader.add_value("bathroom_count", bath_room.strip().split(" ")[0])
        address = "".join(response.xpath("//dl/dt[. ='Adres']/following-sibling::dd/text()").extract())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))


        zipcode = "".join(response.xpath("//dl/dt[. ='Postcode']/following-sibling::dd[1]/text()").extract())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        city = "".join(response.xpath("//dl/dt[. ='Plaats']/following-sibling::dd[1]/text()").extract())
        if city:
            city = re.sub("\s{2,}", " ", city)
            item_loader.add_value("city", city.strip())

        from datetime import datetime
        from datetime import date
        available_date = " ".join(response.xpath("//li[contains(.,'Beschikbaar')]/text()").getall()).strip()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip().split(' ')[-1].strip(), date_formats=["%d/%m/%Y"], languages=['nl'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        images = [response.urljoin(x)for x in response.xpath("//div[@class='col-md-6']/a/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        
        maps =  response.xpath("//script[@type='text/javascript']//text()[contains(.,'lat') and contains(.,'lng')]").extract_first()
        if maps:
            latitude = maps.split("lat:")[1].split(",")[0].strip()
            longitude = maps.split("lng:")[1].split("};")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        desc = "".join(response.xpath("//div[@id='overview']//p[1]//text()").getall())
        item_loader.add_value("description", desc.replace("\n","").replace("\r",""))

        balcony = response.xpath("//ul/li[contains(.,'Balkon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        parking = response.xpath("//ul/li[contains(.,'Parkeren')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        terrace = response.xpath("//ul/li[contains(.,'terras')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        elevator = response.xpath("//ul/li[. =' Lift']/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        furnished = response.xpath("//ul[@class='list-group  icon-text-info']//li//text()[contains(.,'gemeubileerd')]").get()
        if furnished:
            item_loader.add_value("furnished", False)
        else:
            item_loader.add_value("furnished", True)


        item_loader.add_value("landlord_phone", "+31(0)24-3481900")
        item_loader.add_value("landlord_email", "info@24homerentals.nl")
        item_loader.add_value("landlord_name", "24Home Rentals")
        yield item_loader.load_item()
