# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from python_spiders.loaders import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import dateparser
import math
import re

class MySpider(Spider):
    name = "koopsmakelaardij_nl"
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1
    external_source = "Koopsmakelaardij_PySpider_netherlands_nl"
    def start_requests(self):
        start_urls = [
            {"url": "https://koopsmakelaardij.nl/huurwoningen.html?page=1"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for follow_url in response.css("a.o-media__link::attr(href)").extract():
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://koopsmakelaardij.nl/huurwoningen.html?page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//h3//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)

        price = response.xpath("//tr[td[.='Huurprijs']]/td[2]/text()").extract_first()
        if price:
            item_loader.add_value("rent", price.split("€")[1].split(",")[0])
            item_loader.add_value("currency", "EUR")

        property_type = response.xpath("//tr[td[.='Soort woning']]/td[2]/text()").get()
        if property_type:
            if "Appartement" in property_type:
                property_type = "apartment"
                item_loader.add_value("property_type", property_type)
            elif "Studio" in property_type:
                property_type = "studio"
                item_loader.add_value("property_type", property_type)
            elif "Bovenwoning" in property_type or "Benedenwoning" in property_type:
                property_type = "apartment"
                item_loader.add_value("property_type", property_type)
            elif "Hoekwoning" in property_type or "Eengezinswoning" in property_type or "Tussenwoning" in property_type:
                property_type = "house"
                item_loader.add_value("property_type", property_type)
            else:
                return
        else:
            return
            

        square = response.xpath("//tr[td[.='Woonoppervlakte']]/td[2]/text()[.!='m2']").get()
        if square:
            square = square.split("m2")[0].strip()
            if "-" in square:
                square = square.split("-")[1]
            item_loader.add_value("square_meters", str(math.ceil(float(square))))

        images = [response.urljoin(x)for x in response.xpath("//div[contains(@class,'c-listing-gallery__image')]/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_xpath("room_count","//tr[td[.='Kamers']]/td[2]/text()")

        available_date = "".join(response.xpath("//tr[td[.='Beschikbaar per']]/td[2]/text()[not(contains(.,'Per direct'))]").extract())
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %B %Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        
        bathroom=response.xpath("//h5[@class='u-caps']//text()[contains(.,'badkamer')]").get()
        if bathroom:
            bathroom=bathroom.split("badkamer")[0].strip().split(" ")[-1]
            item_loader.add_value("bathroom_count", bathroom)
        
        desc = "".join(response.xpath("//div[@class='c-listing-description__body js-listing-desc-body u-textSmaller']//text()").extract())
        if desc:
            item_loader.add_value("description", desc)
            
        if " etage" in desc:
            floor=desc.split(" etage")[0].strip().split(" ")[-1].replace("appartement","").replace("fietsenberging.","").replace(")","")
            floor=floor_trans(floor)
            if floor:
                item_loader.add_value("floor",floor.strip())

        if "zwembad" in desc.lower():
            item_loader.add_value("swimming_pool", True)
        
        terrace = "".join(response.xpath("//tr[td[.='Buiten']]/td[2]//text()").extract()).strip()
        if terrace:
            if "Balcony" in terrace or "Balkon" in terrace:
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)
            if "Dakterras" in terrace:
                item_loader.add_value("terrace", True)
            
        dishwasher =  "".join(response.xpath("//div[contains(@class,'c-listing-description__body')]//text()[contains(.,'vaatwasmachine') or contains(.,'vaatwasser') ]").extract())
        if dishwasher:
            item_loader.add_value("dishwasher", True)  

        terrace = response.xpath("//tr[td[.='Opleveringsniveau']]/td[2]//text()").get()
        if terrace:
            item_loader.add_value("furnished", True)
        
        item_loader.add_xpath("address", "//tr[td[.='Regio']]/td[2]/text()")
        item_loader.add_xpath("city", "//tr[td[.='Regio']]/td[2]/text()")

        latlng = response.xpath("//div[@class='c-listing-detail']/script/text()").re_first(r"center: \[(.*)\],")
        if latlng:
            lat, lng = latlng.split(",")
            if lat:
                item_loader.add_value("latitude", lat.strip())
            if lng:
                item_loader.add_value("longitude", lng.strip())
           
                
        water_cost=response.xpath("//p/strong/strong[contains(.,'water') and contains(.,'€')]/text()").get()
        if water_cost:
            item_loader.add_value("water_cost", water_cost.split("€")[1].split(",")[0].strip())
        
        item_loader.add_xpath("landlord_phone", "normalize-space(//p[@class='u-textSmaller']/span/a/text())")
        item_loader.add_xpath("landlord_email", "normalize-space(//p[@class='u-textSmaller']/span[2]/a/text())")
        item_loader.add_value("landlord_name", "Koops Makelaardij Haarlem")

        status=response.xpath("//div[@class='o-wrap']/span[contains(@class,'verhuurd')]/text()").get()
        if status:
            return
        else:
            yield item_loader.load_item()

    # 3. PAGINATION LEVEL 1
    def paginate(self, response):
        next_page_url = response.css(
            "div.c-pagination a::attr(href)"
        ).extract_first()  # pagination("next button") <a> element here
        if next_page_url is not None:
            return response.follow(next_page_url, self.parse)

def floor_trans(floor):
    
    if floor.replace("e","").replace("ste","").isdigit():
        return floor.replace("e","")
    elif "eerste" in floor.lower():
        return "1"
    elif "tweede" in floor.lower():
        return "2"
    elif "derde" in floor.lower():
        return "3"
    elif "vierde" in floor.lower():
        return "4"
    elif "vijfde" in floor.lower():
        return "5"
    elif "achtste" in floor.lower():
        return "8"
    elif "bovenste" in floor.lower() or "hoogste" in floor.lower():
        return "upper"
    else :
        return False

    