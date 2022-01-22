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
import re
from datetime import datetime
import dateparser
class MySpider(Spider):
    name = 'songproperties_com_au'   
    execution_type='testing'
    country='australia'
    locale='en'   
    start_urls = ["https://app.inspectrealestate.com.au/External/ROL/QuickWeb.aspx?AgentAccountName=SongProperties"]

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='div-body']/div[@class='divBorder']/div[@class='outer']/div[@class='divInfo']/div[@class='divInspectionButton']/input/@onclick").getall():
            follow_url = response.urljoin(item.split("('")[-1].split("');")[0])
            yield Request(follow_url,callback=self.populate_item)

    # 2. SCRAPING level 2 
    def populate_item(self,response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        externallink=response.url
        if externallink and "login" in externallink:
            return
        externalid=externallink
        if externalid:
            externalid=externalid.split("UniqueID=")[-1].split("&")[0]
            if len(externalid)<80:
                item_loader.add_value("external_id",externalid)
        item_loader.add_value("external_source", "Songproperties_Com_PySpider_australia")
        title = response.xpath("//span[@id='lblPropertyAddress']//text()").get()
        if title:
           item_loader.add_value("title", title)

        
        rent = response.xpath("//span[@id='lblRent']/text()").get()
        if rent:
            price = rent.split("$")[1].split(" ")[0].replace(",","")
            if len(str(price))<2:
                item_loader.add_value("rent",None)

            else:
                item_loader.add_value("rent", int(price)*4)
        item_loader.add_value("currency", "AUD")
        room_count = response.xpath("//span[@id='lblBeds']//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        bathroom_count = response.xpath("//span[@id='lblBaths']//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count) 
        # latitude=response.xpath("//a[@title='Bing Maps']/@href").get()
        # if latitude:
        #     item_loader.add_value("latitude",latitude.split("pos=")[-1].split(",")[-1].split("&")[0])
        # longitude=response.xpath("//a[@id='hlMapLink']/@href").get()
        # if longitude and "pos" in longitude:
        #     item_loader.add_value("longitude",longitude.split("pos=")[-1].split(",")[0])
        available_date = response.xpath("//span[@id='lblAvailableDate']/span/following-sibling::text()").get()
        if available_date:
            available_date=available_date.strip()
            date_parsed = dateparser.parse(
                        available_date, date_formats=["%m/%d/%Y"]
                    )
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        parking = response.xpath("//span[@id='lblCars']//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        address = response.xpath("//span[@id='lblPropertyAddress']//text()").get()
        if address:
            item_loader.add_value("address",address)
        city=response.xpath("//span[@id='lblPropertyAddress']/span/text()").get()
        if city:
            item_loader.add_value("city",city.strip())
        zipcode=response.xpath("//td//a[@id='hlMapLink']/@href[not(contains(.,'https://app'))]").get()
        if zipcode:
            zipcode=zipcode.split("-")[-2:]
            if len(zipcode)<14:
                item_loader.add_value("zipcode",zipcode)
        images = [x for x in response.xpath("//img[@id='imgProperty']/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "SONG GROUP")
        item_loader.add_value("landlord_phone", "(07) 3003 0880")
        item_loader.add_value("landlord_email", "info@songproperties.com.au")
        desc=response.xpath("//td//a[@id='hlMapLink']/@href").get()

        if desc:
            desc_url=desc
            yield Request(
                desc_url, 
                callback=self.desc_item, 
                meta={'item': item_loader}
            )
        
    def desc_item(self, response):
        
        
        item_loader = response.meta.get("item")
        rentcheck=item_loader.get_output_value("rent")
        if not rentcheck:
            rent=response.xpath("//span[@id='lblPropertyPrice']/text()").get()
            if rent:
                rent=rent.strip().split(" ")[-1].replace("$","").split("p")[0]
                
                item_loader.add_value("rent",rent)
        desc1=response.xpath("//section[@class='description']//p//text()").getall()
        if desc1:
            item_loader.add_value("description",desc1)
        desc2=response.xpath("//span[@id='lblPropertyDescription']//text()").getall()
        if desc2:
            item_loader.add_value("description",desc2)
        prop_type=item_loader.get_output_value("description")
        property_type = get_p_type_string(prop_type) 
        if property_type:
            item_loader.add_value("property_type",property_type) 
        latitude=response.xpath("//script[contains(.,'propertyAddress = {')]//text()").get()
        if latitude:
            lat = latitude.split("propertyAddress = {")[-1].split('"lat": "')[-1].split('"')[0]
            lng = latitude.split("propertyAddress = {")[-1].split('"long": "')[-1].split('"')[0]
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude",lng)
        address = response.xpath("//span[@id='lblPropertyAddress']//text()").get()
        if address:
            
            if not item_loader.get_collected_values("zipcode"):
                zipcode = " ".join(address.strip().split(" ")[-2:]).replace(",","")
                item_loader.add_value("zipcode",zipcode)

        yield item_loader.load_item()


        

        # id=response.xpath("//meta[contains(@name,'apple-itunes-app')]/@content").extract()
        # id=id[-1].split("?Id=")[-1]
        # title=response.xpath("//span[@id='lblPropertyAddress']//text()").get()
        # if title:
        #     title=title.replace(" ","-").replace("/","-")
        # follow_url = f"https://propertywebbooks.com.au/book-rental/{id}/{title}"
        # if follow_url:
        #     yield Request(follow_url,callback=self.populate_itemm)

        


  



def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None
