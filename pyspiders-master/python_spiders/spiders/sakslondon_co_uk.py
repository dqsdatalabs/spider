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
    name = 'sakslondon_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['http://www.sakslondon.co.uk/properties.aspx?mode=1&menuID=30']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 1)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'item col')]"):
            follow_url = response.urljoin(item.xpath("./div/a/@href").get())
            status = "".join(item.xpath("./div[contains(@class,'status')]/text()").getall())
            if "let" not in status.lower() and "under" not in status.lower():
                yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            total_page = response.xpath("//div[@class='pagination']//li[last()]//text()").get()
            formdata = {
                "__EVENTTARGET": "ctl00$ContentPlaceHolderMain$repPages$ctl01$lnkPage",
                "__EVENTARGUMENT": "",
                "__LASTFOCUS":"",
                "__VIEWSTATE": response.xpath("//input[@id='__VIEWSTATE']/@value").get(),
                "__VIEWSTATE": response.xpath("//input[@id='__VIEWSTATE']/@value").get(),
                "__VIEWSTATEGENERATOR": response.xpath("//input[@id='__VIEWSTATEGENERATOR']/@value").get(),
                "__EVENTVALIDATION": response.xpath("//input[@id='__EVENTVALIDATION']/@value").get(),
                "ctl00$ContentPlaceHolderMain$lstSort": "Sort Highest Price",
                "ctl00$ContentPlaceHolderMain$cboPageNos": f"Page {page} of {total_page}",
            }
            yield FormRequest(response.url, dont_filter=True, formdata=formdata, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        
        desc = "".join(response.xpath("//div[@id='tabDescription']/p//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: return
        item_loader.add_value("external_source", "Sakslondon_Co_PySpider_united_kingdom")
        item_loader.add_xpath("external_id", "//span[@id='ctl00_ContentPlaceHolderMain_lblPropertyID']/text()")
        title = response.xpath("//h2/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

            if "unfurnished" in title.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in title.lower():
                item_loader.add_value("furnished", True)
        available_date = response.xpath("//h2[contains(.,'Available') and not(contains(.,'Now'))]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Available")[1].split("-")[0], date_formats=["%d %B %Y"], languages=['fr'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        address = response.xpath("//h1/text()").get()
        if address:
            if "," in address:
                if address.count(",") == 2:
                    zipcode = address.split(",")[-1].strip()
                    item_loader.add_value("zipcode", zipcode)
                else:
                    zipcode = address.strip().split(" ")[-1]
                    if not zipcode.isalpha():
                        item_loader.add_value("zipcode", zipcode)
            else:
                zipcode = address.strip().split(" ")[-1]
                if not zipcode.isalpha():
                    item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", address.strip())
            if address.count(",")>0:
                item_loader.add_value("city", address.split(",")[-2].strip())
            else: 
                item_loader.add_value("city", address.split(" ")[-2])

        item_loader.add_xpath("room_count","//li[i[@class='icon-bedrooms']]/text()")
        item_loader.add_xpath("bathroom_count","//li[i[@class='icon-bathrooms']]/text()")
        square_meters = response.xpath("//li[i[@class='icon-area']]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0])

        rent = response.xpath("//div[contains(@class,'price')]/span/text()[contains(.,'£')]").get()
        if rent:
            rent = rent.split("£")[-1].split("p")[0].replace(",","").strip()
            item_loader.add_value("rent", str(int(rent)*4))
            item_loader.add_value("currency", "GBP")
     
        description = " ".join(response.xpath("//span[contains(@id,'PropertyMainDescription')]//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        images = [x for x in response.xpath("//div[@id='property-detail-large']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
 
        balcony = response.xpath("//li/text()[contains(.,'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)   
        parking = response.xpath("//li/text()[contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True) 
        dishwasher = response.xpath("//li/text()[contains(.,'Dishwasher') or contains(.,'dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)  
        washing_machine = response.xpath("//li/text()[contains(.,'washing machine')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)  
        item_loader.add_value("landlord_name", "SAKS LONDON")
        item_loader.add_value("landlord_phone", "0203 7288111")
        item_loader.add_value("landlord_email", "lettings@sakslondon.co.uk")
        lat_lng = response.xpath("//div[@id='tabStreetView']/iframe/@src").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split("&cbll=")[-1].split(",")[0])
            item_loader.add_value("longitude", lat_lng.split("&cbll=")[-1].split(",")[1].split("&")[0])
        commercial = response.xpath('//span[@id="ctl00_ContentPlaceHolderMain_lblPropertyType"]/text()').get()
        if 'Commercial' in commercial:
            return
        else:
            yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None