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
    name = 'ballarat_harcourts_com_au'
    execution_type='testing'
    country='australia'
    locale='en'    
    thousand_separator = ','
    scale_separator = '.'       
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://ballarat.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=13&min=&max=&minbed=&maxbed=",
                    "https://ballarat.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=3&min=&max=&minbed=&maxbed=",
                    "https://ballarat.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=4&min=&max=&minbed=&maxbed=",
                    "https://ballarat.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=6&min=&max=&minbed=&maxbed=",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://ballarat.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=1&min=&max=&minbed=&maxbed=",
                    "https://ballarat.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=2&min=&max=&minbed=&maxbed=",
                    "https://ballarat.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=5&min=&max=&minbed=&maxbed=",
                    "https://ballarat.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=7&min=&max=&minbed=&maxbed=",
                    "https://ballarat.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=61&min=&max=&minbed=&maxbed=",
                    "https://ballarat.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=11&min=&max=&minbed=&maxbed=",
                    "https://ballarat.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=12&min=&max=&minbed=&maxbed=",
                    "https://ballarat.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=14&min=&max=&minbed=&maxbed=",
                    "https://ballarat.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=15&min=&max=&minbed=&maxbed=",
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://ballarat.harcourts.com.au/Property/Rentals?pageid=-2&search=&formsearch=true&OriginalTermText=&OriginalLocation=&location=&proptype=10&min=&max=&minbed=&maxbed=",
                ],
                "property_type": "studio"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//h2/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Ballarat_Harcourts_PySpider_australia")
        title = response.xpath("//div[@id='detailTitle']/h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        external_id = response.xpath("//span[strong[.='Listing Number:']]/text()[normalize-space()]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        lat_lng = response.xpath("//div[h2[.='Property Location']]/iframe/@src").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split("&center=")[-1].split(",")[0])
            item_loader.add_value("longitude", lat_lng.split("&center=")[-1].split(",")[1].split("&")[0])

        item_loader.add_xpath("deposit", "//li[span[.='Bond $: ']]/text()[normalize-space()]")
       
        available_date = response.xpath("//li[span[.='Available Date: ']]/text()[normalize-space()]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"], languages=['en'])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        address = response.xpath("//div[@id='pageTitle']/h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[0].strip())

        item_loader.add_xpath("room_count","//li[@class='bdrm']/span/text()")
        item_loader.add_xpath("bathroom_count","//li[@class='bthrm']/span/text()")
        
        rent = response.xpath("//h3[@id='listingViewDisplayPrice']/text()[normalize-space()]").get()
        if rent:
            rent = "".join(filter(str.isnumeric, rent.split('.')[0].replace(',', '').replace('\xa0', '')))
            item_loader.add_value("rent", str(int(rent)*4))
            item_loader.add_value("currency", "AUD")
     
        description = " ".join(response.xpath("//div[@class='read-more-wrap internet-body']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        images = [x for x in response.xpath("//div[@class='listing-images-carousel']/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            
        parking = response.xpath("//li[@class='grge']/span/text() | //li[contains(@title,'Carport')]/span/text()[.!='0']").get()
        if parking:
            if parking.strip() == "0":
                item_loader.add_value("parking", False) 
            else:
                item_loader.add_value("parking", True) 

        landlord_name = response.xpath("//li[@class='agentContent']//h3/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())  
        landlord_phone = response.xpath("//li[@class='agentContent']//a[contains(@href,'tel')]/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        item_loader.add_value("landlord_email", "ballarat@harcourts.com.au")
        
        yield item_loader.load_item()