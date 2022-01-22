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
    name = 'rogermickhail_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://rogermickhail.com.au/property.html?offset=0&SalesCategoryID=RESIDENTIAL_LEASE&PropertyTypeID=3",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://rogermickhail.com.au/property.html?offset=0&SalesCategoryID=RESIDENTIAL_LEASE&PropertyTypeID%5B0%5D=6&PropertyTypeID%5B1%5D=1&PropertyTypeID%5B2%5D=2",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 6)
        seen = False

        for item in response.xpath("//div[contains(@class,'box-property-list')]"):
            url = item.xpath("./a/@href").get()
            city = item.xpath(".//tr/td[1]/div[@class='text']/text()").get()
            seen = True
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"],"city":city})
        
        if page == 6 or seen:
            follow_url = response.url.replace("offset=" + str(page - 6), "offset=" + str(page))
            yield Request(follow_url, callback=self.parse, meta={"property_type": response.meta["property_type"], "page": page + 6})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Rogermickhail_Com_PySpider_australia")   
        item_loader.add_xpath("external_id", "//tr[td[div[.='Property ID']]]/td[2]//text()")   
        item_loader.add_xpath("title", "//h1/text()")   
        item_loader.add_xpath("address", "//h1/text()")           
        city = response.meta.get('city')
        if city:
            item_loader.add_value("city", city)           
        room_count = response.xpath("//tr/td[div/img[@title='Bedrooms']]/preceding-sibling::td[1]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//tr/td/div[.='Studio']/text()").get()
            if room_count:
                item_loader.add_value("room_count", "1")

        item_loader.add_xpath("bathroom_count", "//tr/td[div/img[@title='Bathrooms']]/preceding-sibling::td[1]//text()")
     
        parking = response.xpath("//tr/td[div/img[@title='Parking']]/preceding-sibling::td[1]//text()").get()
        if parking:
            if parking == "0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
  
        balcony = response.xpath("//div[contains(@class,'box-property-detail-desc')]/text()[contains(.,'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        deposit = response.xpath("//tr[td[div[.='Bond']]]/td[2]//text()").get()
        if deposit:
            deposit = deposit.split("$")[-1].replace(",","").strip()
            item_loader.add_value("deposit", int(float(deposit)))
        rent = response.xpath("//div[@id='mlk-26']/h2/text()[not(contains(.,'CONTACT'))]").get()
        if rent:
            rent = rent.split("$")[1].strip().split(" ")[0].split("-")[0].replace(",","").strip()  
            item_loader.add_value("rent", int(float(rent))*4)
    
        item_loader.add_value("currency", "AUD")
        desc = " ".join(response.xpath("//div[contains(@class,'box-property-detail-desc')]/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        available_date = response.xpath("//tr[td[div[.='Available']]]/td[2]//text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        images = [x for x in response.xpath("//div[@class='imagelightbox-thumbs-inner']//a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        if not item_loader.get_collected_values("images"):
            images = [response.urljoin(x) for x in response.xpath("//div[@class='imagelightbox']//img/@src").getall()]
            if images: item_loader.add_value("images", images)
        
        if not item_loader.get_collected_values("rent"):
            rent = response.xpath("//h2[@class='heading']/text()").get()
            if rent:
                item_loader.add_value("rent", "".join(filter(str.isnumeric, rent)))
                item_loader.add_value("currency", "USD")
      
        item_loader.add_value("landlord_name", "Roger Mickhail Property")
        item_loader.add_value("landlord_phone", "02 9713 5900")
        item_loader.add_value("landlord_email", "enquiries@rogermickhail.com.au")
        yield item_loader.load_item()