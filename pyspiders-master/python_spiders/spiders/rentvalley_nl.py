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
    name = 'rentvalley_nl'
    start_urls = ['https://www.rentvalley.nl/nl/realtime-listings/consumer'] 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)
        for item in data:
            follow_url = response.urljoin(item["url"])
            lat = item["lat"]
            lng = item["lng"]
            property_type = item["mainType"]
            if "apartment" in property_type:
                yield Request(follow_url,callback=self.populate_item, meta={'lat':lat, 'lng':lng, 'property_type': "apartment"})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Rentvalley_PySpider_" + self.country + "_" + self.locale)
     
        lat=response.meta.get("lat")
        lng=response.meta.get("lng")

        item_loader.add_value("external_link", response.url)

        title = response.xpath("normalize-space(//h1/text())").extract_first()
        item_loader.add_value("title", title)
        
        price=response.xpath("//dl[@class='details-simple']/dt[.='Prijs']/following-sibling::dd[1]/text()[1]").extract_first()
        if price:
            item_loader.add_value("rent",price.split("€")[1].split("p")[0])
            item_loader.add_value("currency","EUR")
        
        deposit=response.xpath("//dl[@class='details-simple']/dt[.='Borg']/following-sibling::dd[1]/text()[1]").extract_first()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[1])

       
        available_date =response.xpath("//dl[@class='details-simple']/dt[.='Beschikbaar per']/following-sibling::dd[1]/text()[1][ .!='Per direct']").extract_first()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        item_loader.add_value("property_type", response.meta.get("property_type"))

        zipcode = response.xpath("//dl[@class='details-simple']/dt[.='Postcode']/following-sibling::dd[1]/text()[1]").extract_first()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(" ")[0])
        
        room_count=response.xpath("//dl[@class='details-simple']/dt[.='Kamers']/following-sibling::dd[1]/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count",room_count)
            
        square_meters=response.xpath("//dl[@class='details-simple']/dt[.='Oppervlakte']/following-sibling::dd[1]/text()").extract_first()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)        
        
        desc="".join(response.xpath("//p[@class='object-description']/text()").extract())
        if desc:        
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description",desc)

        utilities = response.xpath("//p[@class='object-description']/text()[contains(.,'Servicekosten')]").extract_first()
        if utilities:  
            try:
                utilities = utilities.split(":")[1].split(",")[0] 
                item_loader.add_value("utilities",utilities.strip())
            except:
                pass           

        item_loader.add_xpath("bathroom_count","//dl[@class='details-simple']/dt[.='Badkamers']/following-sibling::dd[1]/text()")
        
        images = [response.urljoin(x)for x in response.xpath("//div[@class='responsive-slider-slide']/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        pets=response.xpath("//dl[@class='details-simple']/dt[.='Huisdieren']/following-sibling::dd[1]/text()[1]").extract_first()
        if pets:
            if "nee" in pets.lower():
                item_loader.add_value("pets_allowed", False)
            else:
                item_loader.add_value("pets_allowed", True)

        terrace = response.xpath("//dl[@class='details-simple']/dt[.='Inrichting']/following-sibling::dd[1]/text()[1][. !='Unfurnished']").get()
        if terrace:
            item_loader.add_value("furnished", True)

        terrace = response.xpath("//dl[@class='details-simple']/dt[.='Balkon']/following-sibling::dd[1]/text()[1][contains(.,'Ja')]").get()
        if terrace:
            item_loader.add_value("furnished", True)

        item_loader.add_value("latitude", str(lat))
        item_loader.add_value("longitude", str(lng)) 
        item_loader.add_value("address",title.split(",")[0].strip())
        item_loader.add_value("city",title.split(",")[1].strip())


        item_loader.add_value("landlord_phone", "+31(0)70 820 11 96")
        item_loader.add_value("landlord_name", "Rent Valley")

        yield item_loader.load_item()