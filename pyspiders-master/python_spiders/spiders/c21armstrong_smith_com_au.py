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
    name = 'c21armstrong_smith_com_au'
    execution_type='testing'
    country='australia'
    locale='en'  
    thousand_separator = ','
    scale_separator = '.'       
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://bondijunction.century21.com.au/local-properties-for-rent?page=1&types=Apartment&searchtype=lease",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://bondijunction.century21.com.au/local-properties-for-rent?page=1&types=House%7CSemi%2FDuplex&searchtype=lease",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@class='result-list']/ul/li/div/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

        if page == 2 or seen:
            follow_url = response.url.replace("page=" + str(page - 1), "page=" + str(page))
            yield Request(follow_url, callback=self.parse, meta={"property_type": response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)        
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "C21armstrong_Smith_Com_PySpider_australia")
        item_loader.add_value("external_id", response.url.split("/")[-1])
        title = "".join(response.xpath("//h1//span/text()").getall())
        if title:
            item_loader.add_value("title", title.strip())
        zipcode = " ".join(response.xpath("//meta[@property='og:region']/@content | //meta[@property='og:postal-code']/@content").getall())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        city = response.xpath("//h1//span[@class='suburb block']/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
       
        address = response.xpath("//h1//span[@class='streetaddress']/text()").get()
        if address:
            if city:
                address = address.strip()+", "+city.strip()
            item_loader.add_value("address", address.strip())
        item_loader.add_xpath("room_count","//span[i[@class='icon-bed']]/text()")
        item_loader.add_xpath("bathroom_count","//span[i[@class='icon-bath']]/text()")
        item_loader.add_xpath("deposit","//div[b[.='Bond']]/text()")
        rent = response.xpath("//h2/div[@class='pricetext']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)

        available_date = response.xpath("//div[b[.='Availability']]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        swimming_pool = response.xpath("//li/span[.='Pool']/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        parking = response.xpath("//span[i[@class='icon-car']]/text()").get()
        if parking:
            if parking.strip() =="0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        furnished = response.xpath("//li/span[.='Furnished' or .='furnished']/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
     
        description = " ".join(response.xpath("//div[contains(@class,'column')]/div[@class='contentRegion']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        images = [x for x in response.xpath("//div[@id='slideshow']//span/link/@href").getall()]
        if images:
            item_loader.add_value("images", images)
     
        item_loader.add_xpath("landlord_name", "//li[div/div[@class='agent']][1]//div[@class='name']/text()")
        item_loader.add_xpath("landlord_phone", "//li[div/div[@class='agent']][1]//a[@itemprop='telephone']/text()")
        item_loader.add_xpath("landlord_email", "//li[div/div[@class='agent']][1]//a[@itemprop='email']/text()")

        item_loader.add_xpath("latitude", "//meta[@property='place:location:latitude']/@content")
        item_loader.add_xpath("longitude", "//meta[@property='place:location:longitude']/@content")
        yield item_loader.load_item()