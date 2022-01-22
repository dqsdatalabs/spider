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


class MySpider(Spider):
    name = 'perfectrent_nl'
    execution_type='testing'
    country='netherlands'
    locale='nl'
    external_source='Perfectrent_PySpider_netherlands'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.perfectrent.nl/nl/huren/aanbod/rentals"}           
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                        )

    # 1. FOLLOWING
    def parse(self, response):

        for follow_url in response.xpath("//div[contains(@class,'list-object ') and not(contains(.,'Verhuurd'))]//a[@class='list-object-address']/@href").extract():
            yield Request(follow_url, callback=self.populate_item)
        
        pagination = response.xpath("//ul[@class='pagination']//a[contains(.,'Volgende')]/@href").get()
        if pagination:
            yield Request(response.urljoin(pagination), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        p_type = response.xpath("//dl/dt[contains(.,'Type')]/following-sibling::dd[1]/text()").get()
        if p_type:
            if "Appartement" in p_type:
                item_loader.add_value("property_type", "apartment")
            elif "woning" in p_type:
                item_loader.add_value("property_type", "house")
            elif "Kamer" in p_type:
                item_loader.add_value("property_type", "room")
                item_loader.add_value("room_count", "1")
            else:
                return

        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_value("external_source", "Perfectrent_PySpider_netherlands")

        rent = "".join(response.xpath("//dl[dt[.='Huurprijs']]/dd[contains(.,'€')]/text()").extract())
        if rent:
            price =  rent.split(",")[0].strip().replace(".","")
            item_loader.add_value("rent_string", price)


        meters = "".join(response.xpath("//dl[dt[.='Oppervlakte']]/dd[contains(.,'m²')]/text()").extract())
        if meters:
            item_loader.add_value("square_meters", meters.split("m²")[0].strip())

        room = "".join(response.xpath("//dl/dt[contains(.,'Slaapkamers')]/following-sibling::dd[1]/text()").extract())
        if room:
            item_loader.add_value("room_count", room.strip())
  
        available_date=response.xpath("//dl/dt[contains(.,'Beschikbaar per')]/following-sibling::dd[1]/text()[.!='direct']").get()

        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        city = "".join(response.xpath("//dl/dt[contains(.,'Plaats')]/following-sibling::dd[1]/text()").extract())
        if city:
            item_loader.add_value("city", city.strip())

        address = response.xpath("//dl/dt[contains(.,'Wijk')]/following-sibling::dd[1]/text()").extract_first()
        if address:
            item_loader.add_value("address", "{} {}".format(address,city))
        else:
            item_loader.add_value("address", city)

        desc = "".join(response.xpath("//p[@class='object-description']/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        deposit = response.xpath("//text()[contains(.,'waarborgsom') and contains(.,'€')]").get()
        if deposit:
            deposit = deposit.split("€")[1].strip()
            item_loader.add_value("deposit", deposit)

        images = [x.replace(" ", "%20") for x in response.xpath("//div[@class='photo-slider-slides']//img//@src").extract()]
        if images is not None:
            item_loader.add_value("images", images) 

        LatLng = "".join(response.xpath("//div[@class='simple-map-markers']//text()").extract())  
        if LatLng:
            item_loader.add_value("latitude", LatLng.split('"lat":')[1].split(",")[0].strip())
            item_loader.add_value("longitude", LatLng.split('"lng":')[1].split("}")[0].strip())

        furnished = "".join(response.xpath("//dl/dt[contains(.,'Interieur')]/following-sibling::dd[1]/text()").extract())
        if furnished:
            item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_phone", "+31 10 302 70 70")
        item_loader.add_value("landlord_name", "Perfect Rent")
        item_loader.add_value("landlord_email", "contact@perfectrent.nl")    
        

        yield item_loader.load_item()