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
    name = 'bourkes_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    start_urls = ['https://bourkes.com.au/listings/?post_type=listings&count=20&orderby=meta_value&meta_key=dateListed&sold=0&saleOrRental=Rental&order=dateListed-desc&paged=1&extended=1&minprice=&maxprice=&minbeds=&maxbeds=&baths=&cars=&type=residential&externalID=&subcategory=&landsize=']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        data = response.xpath("//script[contains(.,'MapDataStore ')]/text()").get()
        data = "["+data.split("= [")[1].split("}];")[0]+"}]"
        data_json = json.loads(data)
        for item in data_json:
            lat = item["Lat"]
            lng = item["Long"]
            address = item["address"]
            price = item["price"]
            yield Request(item["url"], callback=self.populate_item, meta={"lat":lat,"lng":lng,"address":address,"price":price})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Bourkes_PySpider_australia")
        desc = "".join(response.xpath("//div[contains(@class,'post-content')]//text() | //div[@class='section-header']//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: return
        title = response.xpath("//meta[@property='og:title']/@content").get()
        if title:
            item_loader.add_value("title", title.strip())
        external_id = response.xpath("//section[@id='single-listings-content']//div[strong[.='property ID']]/text()[normalize-space()]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        item_loader.add_value("latitude", response.meta.get('lat'))
        item_loader.add_value("longitude", response.meta.get('lng'))
        item_loader.add_xpath("deposit", "//section[@id='single-listings-content']//div[strong[.='bond price']]/text()[normalize-space()]")
       
        square_meters = response.xpath("//section[@id='single-listings-content']//div[strong[contains(.,'size')]]/text()[normalize-space()]").get()
        if square_meters:
            square_meters = "".join(filter(str.isnumeric, square_meters.split(".")[0]))
            item_loader.add_value("square_meters", str(square_meters))
          
        available_date = response.xpath("//div[strong[contains(.,'date available')]]/text()[normalize-space()]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %B %Y"], languages=['en'])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        address =  response.meta.get('address')
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-1].strip())

        item_loader.add_xpath("room_count","//div[@class='property-info-bar']//p[@class='listing-attr icon-bed']/text()")
        item_loader.add_xpath("bathroom_count","//div[@class='property-info-bar']//p[@class='listing-attr icon-bath']/text()")
        
        rent = response.meta.get('price')
        if rent:
            rent = "".join(filter(str.isnumeric, rent.split('.')[0].replace(',', '').replace('\xa0', '')))
            item_loader.add_value("rent", str(int(rent)*4))
            item_loader.add_value("currency", "AUD")
     
        description = " ".join(response.xpath("//section[contains(@class,'single-listing-description')]/div//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        images = [response.urljoin(x.split("url('")[1].split("')")[0]) for x in response.xpath("//div[@class='single-listing-slick-slides']/div/@style").getall()]
        if images:
            item_loader.add_value("images", images)
 
        balcony = response.xpath("//li/text()[contains(.,'balcony') or contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)   
        parking = response.xpath("//div[@class='property-info-bar']//p[@class='listing-attr icon-car']/text()").get()
        if parking:
            if parking.strip()=="0":
                item_loader.add_value("parking", False) 
            else:
                item_loader.add_value("parking", True) 

        dishwasher = response.xpath("//li/text()[contains(.,'Dishwasher') or contains(.,'dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)  
        washing_machine = response.xpath("//li/text()[contains(.,'washing machine')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)  
        landlord_name = response.xpath("//div[contains(@class,'-details-wrapper')][1]//h5//text()[normalize-space()]").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())  
        landlord_phone = response.xpath("//div[@class='section-body property-agent-details-wrapper'][1]//p[strong[.='office']]//a/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())  
        else:
            item_loader.add_xpath("landlord_phone", "//div[contains(@class,'-details-wrapper')][1]//div[div[p[strong[.='phone']]]]/div/p[@class='agency-tile-text']/text()")
        landlord_email = response.xpath("//div[@class='section-body property-agent-details-wrapper'][1]//p[strong[.='email']]//a/text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.strip())  
        else:
            item_loader.add_xpath("landlord_email", "//div[div[p[strong[.='email']]]]/div/p/text()[normalize-space()]")
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None