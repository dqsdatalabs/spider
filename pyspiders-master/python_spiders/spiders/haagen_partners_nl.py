# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser

class MySpider(Spider):
    name = 'haagen_partners_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'en'
    start_urls = ["https://haagen-partners.nl/huurwoningen/"]

    # 1. FOLLOWING
    def parse(self, response):
        total_page = response.xpath("//div[@class='pagination-meta']/text()").get()
        if total_page:
            total_page = int(total_page.split(":")[1].strip().split("van")[1].strip())
        else:
            total_page = 1
        page = response.meta.get("page", 2)
        for item in response.xpath("//div[@class='aanbod-woning']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        if page <= total_page:
            p_url = f"https://haagen-partners.nl/huurwoningen/?paginate={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Haagen_Partners_PySpider_netherlands") 
        item_loader.add_xpath("title", "//title/text()") 

        f_text = "".join(response.xpath("//h2[@class='single-info']/..//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        
        item_loader.add_value("external_id", response.url.split('/')[-2].strip())

        rent = "".join(response.xpath("substring-before(//div[@class='aanbod-info-price']/text(),' ')").getall())
        if rent:
            price = rent.replace(",",".").replace(".","")
            item_loader.add_value("rent_string",price.strip())
        else:
            item_loader.add_value("currency","EUR")     

        room_count = "".join(response.xpath("substring-after(//div[@class='aanbod-content']/div/p[@class='aanbod-ifo-rooms']/text(),':')").getall())
        if room_count:
            item_loader.add_value("room_count",room_count.strip())

        bathroom_count = "".join(response.xpath("substring-after(//div[@class='aanbod-content']/div/p[@class='aanbod-ifo-baths']/text(),':')").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())       

        meters = "".join(response.xpath("substring-after(//div[@class='aanbod-content']/div/p[@class='aanbod-ifo-squarefeet']/text(),':')").getall())
        if meters:
            item_loader.add_value("square_meters",meters.split("m²")[0].strip())

        city = "".join(response.xpath("//h2[@class='aanbod-info-city']/text()").getall()) 
        if city:
            item_loader.add_value("city", re.sub("\s{2,}", " ", city.strip()))

        address = "".join(response.xpath("//div[@class='aanbod-info-title']//text()").getall()) 
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))

        description = " ".join(response.xpath("//div[@class='wpb_wrapper']/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').strip())

        LatLng = " ".join(response.xpath("//script[contains(.,'position')]/text()").getall()).strip()   
        if LatLng:
            lat = LatLng.split("lat:")[1].split(",")[0].strip()
            lng = LatLng.split("lng:")[1].strip().split(" ")[0].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)

        images = [x for x in response.xpath("//div[@class='carousel-inner']/div//img/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images) 

        furnished = "".join(response.xpath("substring-after(//div[@class='aanbod-content']/div/p[@class='aanbod-ifo-furniture']/text(),':')").getall()) 
        if furnished:
            if "gemeubileerd" in furnished.lower(): item_loader.add_value("furnished", True)
            elif "gestoffeerd" in furnished.lower(): item_loader.add_value("furnished", False)
        
        feature = " ".join(response.xpath("//div[@class='wpb_wrapper']//text()").getall())
        if "Washer" in feature:
            item_loader.add_value("washing_machine", True)
        if "elevator" in feature.lower():
            item_loader.add_value("elevator", True)
        if "balcony" in feature.lower():
            item_loader.add_value("balcony", True)
        if "parking " in feature.lower():
            item_loader.add_value("parking", True)

        item_loader.add_value("landlord_phone", "+31 20 672 33 31")
        item_loader.add_value("landlord_email", "info@haagen-partners.nl")
        item_loader.add_value("landlord_name", "Haagen & Partners")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None