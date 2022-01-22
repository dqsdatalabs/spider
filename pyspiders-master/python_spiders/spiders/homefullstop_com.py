# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'homefullstop_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    start_urls = ['https://www.homefullstop.com/properties.aspx?mode=1&menuID=50']
    
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://www.homefullstop.com",
        "Referer": "https://www.homefullstop.com/properties.aspx?mode=1&menuID=50",
    }
    
    def jump(self,response):
        formdata = {
            "__VIEWSTATEGENERATOR" : response.xpath("//input[@name='__VIEWSTATEGENERATOR']/@value").get(),
            "__VIEWSTATE" : response.xpath("//input[@name='__VIEWSTATE']/@value").get(),
            "__EVENTVALIDATION" : response.xpath("//input[@name='__EVENTVALIDATION']/@value").get(),
            "ctl00$ContentPlaceHolderMain$lstSort": "Sort Highest Price",
            "ctl00$ContentPlaceHolderMain$cboPageNos": "Page 1 of 4",
        }
        
        yield FormRequest(
            url=self.start_urls[0],
            callback=self.parse,
            headers=self.headers,
            dont_filter=True,
            formdata=formdata
        )
    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//div[@class='image']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            formdata = {
                "__VIEWSTATEGENERATOR" : response.xpath("//input[@name='__VIEWSTATEGENERATOR']/@value").get(),
                "__VIEWSTATE" : response.xpath("//input[@name='__VIEWSTATE']/@value").get(),
                "__EVENTVALIDATION" : response.xpath("//input[@name='__EVENTVALIDATION']/@value").get(),
                "ctl00$ContentPlaceHolderMain$lstSort": "Sort Highest Price",
                "ctl00$ContentPlaceHolderMain$cboPageNos": f"Page {page} of 4",
            }
            url = "https://www.homefullstop.com/properties.aspx?mode=1&menuID=50"
            try:
                yield FormRequest(url, callback=self.parse, headers=self.headers, dont_filter=True, formdata=formdata, meta={"page": page+1})
            except: pass

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        if "for-sale" in response.url:
            return
        item_loader.add_value("external_link", response.url)
        
        description = "".join(response.xpath("//span[contains(@id,'Description')]//text()").getall())
        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description))
        else: return
        item_loader.add_value("external_source", "Homefullstop_PySpider_united_kingdom")
        external_id = response.xpath("//p[contains(.,'Property ID')]//span//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        title = response.xpath("//h2/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        address = response.xpath("//h1/text()").get()
        zipcode = ""
        if address:
            item_loader.add_value("address", address.strip())
            if address.count(",") == 1:
                city_zipcode = address.split(",")[-1].strip()
                if not " " in city_zipcode:
                    item_loader.add_value("city", city_zipcode)
                else:
                    zipcode = city_zipcode.split(" ")[-1]
                    if not zipcode.isalpha():
                        city = city_zipcode.split(zipcode)[0].strip()
                        item_loader.add_value("city", city)
                        zipcode = zipcode
                    else: zipcode = ""
            else:
                city_zipcode = address.split(",")[-1].strip()
                if " " in city_zipcode:
                    zipcode = city_zipcode.split(" ")[-1]
                    city = city_zipcode.split(" ")[0]
                    item_loader.add_value("city", city)
                    zipcode = zipcode
                else:
                    city = address.split(",")[-2]
                    item_loader.add_value("city", city)
                    zipcode = city_zipcode

        if zipcode and not zipcode.isalpha():
            item_loader.add_value("zipcode", zipcode)
        
        room_count = response.xpath("//li[i[@class='icon-bedrooms']]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        elif get_p_type_string(description) and get_p_type_string(description) =="studio":
            item_loader.add_value("room_count", "1")
        item_loader.add_xpath("bathroom_count", "//li[i[@class='icon-bathrooms']]/text()")
        rent_string =  response.xpath("//h2/text()[contains(.,'£')]").get()
        if rent_string:
            if "pw" in rent_string.lower():
                rent = rent_string.split("£")[-1].split("p")[0].replace(",","")
                item_loader.add_value("rent", str(int(rent.strip())*4))
                item_loader.add_value("currency", "GBP")
            else:
                item_loader.add_value("rent_string", rent_string)
        description = " ".join(response.xpath("//span[contains(@id,'Description')]//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
    
        square_meters = response.xpath("//li[i[@class='icon-area']]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
   
        lat_lng = response.xpath("//div[@id='tabStreetView']/iframe/@src").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split('cbll=')[1].split(',')[0].strip())
            item_loader.add_value("longitude", lat_lng.split('cbll=')[1].split(',')[1].split('&')[0].strip())

        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'Garage')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        terrace = response.xpath("//li[contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        furnished = response.xpath("//h2/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        floor_plan_images = [x for x in response.xpath("//div[@id='tabFloorPlan']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        images = [x for x in response.xpath("//div[@id='property-detail-large']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "HOME FULLSTOP")
        item_loader.add_value("landlord_phone", "020 7402 4455")
        item_loader.add_value("landlord_email", "info@homefullstop.com")
        yield item_loader.load_item()
    
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    elif p_type_string and "room" in p_type_string.lower():
        return "room"
    else:
        return None