# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from python_spiders.loaders import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import dateparser
import re

class MySpider(Spider):
    name = "rotterdamapartments_com"    
    thousand_separator = ','
    scale_separator = '.'  
    start_urls = ["https://rotterdamapartments.com/en/for-rent"]
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'en' # LEVEL 1


    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        for follow_url in response.xpath("//a[contains(@class,'house-item')]/@href").extract():
            yield response.follow(follow_url, self.populate_item)
        yield self.paginate(response) 

    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Rotterdamapartments_PySpider_" + self.country + "_" + self.locale)

        title = response.xpath("//h1/text()").get()
        title = re.sub('\s{2,}', ' ', title)
        item_loader.add_value("title", title.strip())
        
        item_loader.add_value("external_link", response.url)

        if get_p_type_string(response.url):
            item_loader.add_value("property_type", get_p_type_string(response.url))
        else:
            return

        desc = "".join(response.xpath("//div[@class='house-page-description']/div/text()").extract())
        desc = re.sub('\s{2,}', ' ', desc)
        item_loader.add_value("description", desc.strip())
        if desc and "parking space" in desc.lower():
            return 

        price = response.xpath("//div[@class='col-lg-8']//strong[contains(@class,'house-page-sidebar__price')]/text()").get()
        if price:
            if "," in price:
                price = price.split(",")[0]
            item_loader.add_value(
                "rent_string", price.replace(".",""))
            # item_loader.add_value("currency", "EUR")

        item_loader.add_xpath(
            "external_id", "//div[@class='house-page-specs']//dt[contains(.,'Object')]/following-sibling::dd[1]/text()"
        )

        square = response.xpath(
            "//li[i[contains(@class,'fa-ruler-vertical')]]//text()[contains(.,'m')]"
        ).get()
        if square:
            item_loader.add_value(
                "square_meters", square.split("m")[0]
            )
        room_count = response.xpath(
            "//div[@class='house-page-specs']//dt[contains(.,'Rooms')]/following-sibling::dd[1]/text()"
        ).get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        city = response.xpath("//nav[@class='breadcrumb']/a[3]/text()").get()
        if city:
            item_loader.add_value("city", re.sub('\s{2,}', ' ', city.strip()))

        address = response.xpath("//h1/text()").get()
        if address:
            address = address+", "+city
            item_loader.add_value("address",  re.sub('\s{2,}', ' ', address))
                    
        available_date = "".join(response.xpath(
            "//div[@class='flex-fill'][contains(.,'Available') and not(contains(.,'Direct'))]//text()").getall())
        if available_date:
            available_date = available_date.split(":")[1].strip()
            date_parsed = dateparser.parse(
                available_date
            )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        
        balcony = response.xpath(
            "//ul/li[@class='house-page-extras__item'][contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//ul/li[@class='house-page-extras__item'][contains(.,'terrace') or contains(.,'Terrace') ]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        elevator = response.xpath("//ul/li[@class='house-page-extras__item'][contains(.,'Lift') or contains(.,'lift') ]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        parking = response.xpath("//ul/li[@class='house-page-extras__item'][contains(.,'Parking') or contains(.,'Garage') ]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        item_loader.add_xpath("energy_label", "//div[@class='house-page-specs']//dt[contains(.,'Energy label') and not(contains(.,'status') )]/following-sibling::dd[1]/text()")

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='house-page__photos']//a/@href"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)            
        
        item_loader.add_value("landlord_phone", "+31 (0)10 41 22 221")
        item_loader.add_value("landlord_name", "Rotterdam Apartments")
        item_loader.add_value("landlord_email", "info@rotterdamapartments.com")

        latlng = response.xpath("//script/text()[contains(.,'google.maps.Marker')]").get()
        if latlng:
            latlng = latlng.split("google.maps.Marker")[1].split(");")[0].strip()
            item_loader.add_value("latitude", latlng.split("lat:")[1].split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split("lng:")[1].split("}")[0].strip())
        yield item_loader.load_item()

    # 3. PAGINATION LEVEL 1
    def paginate(self, response):
        next_page_url = response.xpath("//a[@rel='next']/@href").extract_first()  # pagination("next button") <a> element here
        if next_page_url is not None:
            return response.follow(next_page_url, self.parse)
    
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "room" in p_type_string.lower():
        return "room"
    else:
        return None
