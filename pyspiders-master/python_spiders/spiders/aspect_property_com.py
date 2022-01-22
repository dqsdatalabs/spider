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
    name = 'aspect_property_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'

    start_urls = ["https://www.aspect-property.com/api/set/results/grid"]
    form_data = {
        "page": "1",
        "sortorder": "price-desc",
        "RPP": "12",
        "OrganisationId": "37f64c33-b2e4-4e5b-9833-b3ffbb253438",
        "WebdadiSubTypeName": "Rentals",
        "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c},{59c95297-2dca-4b55-9c10-220a8d1a5bed}",
        "includeSoldButton": "false",
    }

    
    def start_requests(self):
        
        yield FormRequest(self.start_urls[0], callback=self.parse, formdata=self.form_data)

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page",2)
        seen = False
        for item in response.xpath("//div[contains(@class,'property-wrapper')]//a[@class='property-description-link']//@href").getall():
           
            # status = item.xpath(".//div[contains(@class,'property-status')]/span/text()").get()
            # if status and "under" in status.lower():
            #     continue
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        if page == 2 or seen:
            form_data = {
                "page": f"{page}",
                "sortorder": "price-desc",
                "RPP": "12",
                "OrganisationId": "37f64c33-b2e4-4e5b-9833-b3ffbb253438",
                "WebdadiSubTypeName": "Rentals",
                "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c},{59c95297-2dca-4b55-9c10-220a8d1a5bed}",
                "includeSoldButton": "false",
                
            }
            f_url="https://www.aspect-property.com/api/set/results/grid"
            yield FormRequest(
                url=f_url,
                callback=self.parse,
                formdata=form_data,
                meta={
                    "page" : page+1,
                }
            )

        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Aspect_Property_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//title/text()")

        f_text = " ".join(response.xpath("//div[@class='accordion-inner']/ul/li/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = " ".join(response.xpath("//section[@id='description']//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return

        address = "".join(response.xpath("//div[contains(@class,'property-address')]//h1/span//text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
        city = "".join(response.xpath("//h1/span[@class='city']/text()").getall())
        if city:
            item_loader.add_value("city", city.replace(",","").strip())
        zipcode = "".join(response.xpath("//h1/span[@class='displayPostCode']/text()").getall())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.replace(",","").strip())

        rent = "".join(response.xpath("//h2/span[@class='nativecurrencyvalue']/text()").getall())
        if rent:
            price = rent.replace(",",".").replace(".","")
            item_loader.add_value("rent",price.strip())
        item_loader.add_value("currency","GBP")
        
        room_count = response.xpath("//ul/li[@class='FeaturedProperty__list-stats-item'][1]/span/text()").get()
        if room_count:
            if room_count !="0":
                item_loader.add_value("room_count",room_count)
            else:
                room_count = "".join(response.xpath("//h2[contains(.,'Studio')]/text()").getall())
                if "studio" in room_count.lower():
                    item_loader.add_value("room_count","1")

        bathroom_count = response.xpath("//ul/li[@class='FeaturedProperty__list-stats-item'][2]/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())

        floor = "".join(response.xpath("//ul[@class='color-white']/li/text()[contains(.,'Floor')]").getall())
        if floor:
            floor = floor.split(" ")[0].strip()

        available_date=response.xpath("//ul[@class='color-white']/li/text()[contains(.,'Available')]").get()

        if available_date:
            date2 =  available_date.split("Available")[1].replace("from","").replace("From","").replace("Immediately","now").strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d"]
            )
            if date_parsed:
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)

        LatLng = "".join(response.xpath("substring-before(substring-after(//section[@id='maps']/@data-cords,'lat'),',')").getall())
        if LatLng:
            item_loader.add_value("latitude",LatLng.replace('": "',"").replace('"',"").strip())
            lng = "".join(response.xpath("substring-before(substring-after(//section[@id='maps']/@data-cords,'lng'),'}')").getall())
            item_loader.add_value("longitude",lng.replace('": "',"").replace('"',"").strip())

        description = " ".join(response.xpath("//section[@id='description']/p/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').strip())

        images = [x.split("(")[1].split(")")[0].strip() for x in response.xpath("//div[@id='imageViewerCarousel']/div/div/@style").extract()]
        if images is not None:
            item_loader.add_value("images", images)

        furnished = "".join(response.xpath("//span[@class='furnished']/text()").getall())
        if furnished:
            if "Unfurnished" in furnished :
                item_loader.add_value("furnished",False)
            elif "Furnished" in furnished:
                item_loader.add_value("furnished",True)

        item_loader.add_value("landlord_phone", "0208 965 0000")
        item_loader.add_value("landlord_email", "info@aspect-property.com")
        item_loader.add_value("landlord_name", "Aspect Property")

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "etage" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None