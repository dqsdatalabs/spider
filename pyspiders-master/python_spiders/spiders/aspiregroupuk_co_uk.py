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

class MySpider(Spider):
    name = 'aspiregroupuk_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source='Aspiregroupuk_PySpider_united_kingdom'
    start_urls = ["http://aspiregroupuk.com/properties-to-let-2/"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='wrapper_properties_ele']"):
            status = item.xpath(".//div[@class='rh_label__wrap']/text()").get()
            if status and ("agreed" in status.lower() or status.strip().lower() == "let"):
                continue
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'View Property')]/@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
    
        f_text = " ".join(response.xpath("//h5//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//div[contains(@class,'content clearfix')]//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else:
            return
        
        
        title = response.xpath("//h1[@class='page-title']/span/text()").get()
        if title:
            item_loader.add_value("title",title.strip())
            item_loader.add_value("address",title.strip())
        else:
            title = response.xpath("//h1[@class='page-title']/span/text()").get()
            if title:
                item_loader.add_value("title",title.strip())
                item_loader.add_value("address",title.strip())
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value=self.external_source, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div/span[contains(.,'Bathroom')]//text()[1]", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@class='slides']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//article/div[contains(@class,'content')]//p//text()[contains(.,'floor')]", input_type="M_XPATH", split_list={"floor":0, " ":-1}, lower_or_upper=1)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={'lat":"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={'lng":"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[@class='features']//li[contains(.,'Balcony')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[@class='features']//li[contains(.,'Terrace')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished') or contains(.,'FURNISHED') and not(contains(.,'Un-Furnished'))]", input_type="F_XPATH", tf_item=True)



        
        rent = "".join(response.xpath("//h5/span[contains(@class,'price')]/text()[contains(.,'£')]").getall())
        price = ""
        if rent:
            if "pw" in rent.lower():
                price = rent.split("£")[1].strip().split(" ")[0]
                item_loader.add_value("rent", int(price)*4)
            else:
                rent = rent.split("£")[1].replace(",","").split(" ")[0]
            item_loader.add_value("rent", rent)
        
        room_count = "".join(response.xpath("//div/span[contains(.,'Bedroom')]//text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.split("\xa0")[0].strip())
        elif "studio" in prop_type: item_loader.add_value("room_count", "1")
        


        from datetime import datetime
        import dateparser
        available_date = response.xpath("//article/div[contains(@class,'content')]//p//text()[contains(.,'Available') or contains(.,'AVAILABLE')]").get()
        if available_date:
            if "IMMEDIATELY" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            
            date_parsed = dateparser.parse(available_date.replace("Available","").replace(".","").strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="ASPIRE GROUP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0116 296 2540", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@aspiregroupuk.com", input_type="VALUE")

            
        desc = " ".join(response.xpath("//div[@class='content clearfix']/p/text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        else:
            desc = " ".join(response.xpath("//div[@class='description-truncate']/div/p/text()").getall())
            if desc:
                item_loader.add_value("description",desc)

        bathroom_count = response.xpath("//span[@class='property-meta-bath']/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split("\xa0")[0]
            item_loader.add_value("bathroom_count",bathroom_count)

        city = response.xpath("//nav/ul/li[a[text()='Home']]/following-sibling::li/a/text()").get()
        if city:
            item_loader.add_value("city",city)


        zipcode = response.xpath("//address[@class='title']/text()").get()
        if zipcode:
            code = re.search(" ([A-Z\da-z]{3} [A-Z\da-z]{3})[\b, ]",zipcode)
            if code:
                item_loader.add_value("zipcode",code.group(1))
            # else:
            #     return
        
        if "USA" in str(response.body):
            return

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "suite" in p_type_string.lower():
        return "room"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "terraced" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None