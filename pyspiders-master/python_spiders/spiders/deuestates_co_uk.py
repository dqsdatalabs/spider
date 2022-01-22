# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from word2number import w2n
import dateparser

class MySpider(Spider):
    name = 'deuestates_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.deuestates.co.uk/search-gallery.php?rental_type=Professional&bedrooms=&availabledate="]

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='details']/a"):
            title = item.xpath("./h2/text()").get()
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"title":title})

        
        next_page = response.xpath("//a[.='>']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                )
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Deuestates_Co_PySpider_united_kingdom")
        title = response.meta["title"]
        item_loader.add_value("title", title)
        prop_type = ""
        if get_p_type_string(title):
            prop_type = get_p_type_string(title)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return
        item_loader.add_xpath("address", "//div[@class='propertyDetails']/address/text()")
        city = response.xpath("//h1/strong/text()").get()
        if city:
            city =city.split(",")[-1].strip()
            if "(" not in city:
                item_loader.add_value("city", city)

        ext_id = response.url.split("&aref=")[1].strip().split("&")[0].split("/")[0]
        if ext_id:
            item_loader.add_value("external_id", ext_id)

        rent = "".join(response.xpath("//p[@class='price']/text()").getall())
        if rent:
            if "pcm" not in rent.lower():
                rent = rent.split(" ")[0].replace(" ","").replace("£","").replace(",","")
                rent = str(int(rent)*4)
            else:   
                rent = rent.split(" ")[0].replace(" ","").replace("£","").replace(",","")     
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency","GBP") 

        room_count = "".join(response.xpath("//div[@class='keyInformation']/p/text()[contains(.,'Bedroom')]").getall())
        if room_count:
            room = room_count.split("Bedroom")[0].replace("-","").strip()
            item_loader.add_value("room_count",room) 

        available_date=response.xpath("substring-after(//div[@class='col-md-8']/h2/text(),'Available ')").get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%d-%m-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)


        bathroom_count = "".join(response.xpath("//div[@class='keyInformation']/p/text()[contains(.,'Bathroom')]").getall())
        if bathroom_count:
            bath = bathroom_count.split("Bathroom")[0].replace("-","").strip()
            item_loader.add_value("bathroom_count",str(w2n.word_to_num(bath))) 

        latlng = "".join(response.xpath("//div[@id='map_canvas_large']/script/text()[contains(.,'latlng')]").extract())
        if latlng:
            lat = latlng.split("google.maps.LatLng(")[1].split(",")[0].strip()
            lng = latlng.split("google.maps.LatLng(")[1].split(",")[1].split(")")[0].strip()
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude",lng)

        energy_label = "".join(response.xpath("substring-before(//div[@class='row justify-content-center']/div/a/@href[contains(.,'EE')],'.png')").getall())
        if energy_label:
            label = energy_label.split("_0")[-1].strip()
            item_loader.add_value("energy_label",label)
            

        description = " ".join(response.xpath("//div[@class='propertyDetails']/p/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').strip())

        images = [x for x in response.xpath("//ul[@class='slides']/li/@data-thumb").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished = "".join(response.xpath("//div[@class='keyInformation']/p/text()[contains(.,'furnished')]").getall())
        if furnished:
            item_loader.add_value("furnished",True)

        parking = "".join(response.xpath("//div[@class='keyInformation']/p/text()[contains(.,'parking ')]").getall())
        if parking:
            item_loader.add_value("parking",True)

        item_loader.add_value("landlord_phone", "0113 275 1010")
        item_loader.add_value("landlord_name", "DEU ESTATES PROPERTIES")
        item_loader.add_value("landlord_email", "lets@deuestates.co.uk")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None