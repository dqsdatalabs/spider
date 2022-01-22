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
    name = 'enrichrealty_com_au'
    execution_type='testing'
    country='australia'
    locale='en' 
    external_source="Enrichrealty_Com_PySpider_australia"

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://enrichrealty.com.au/property-search/?sort=newest&search_status=10&search_address=&search_street_no=&search_street=&search_neighborhood=&search_city=&search_state=&search_zip=&search_type=9&search_beds=0&search_baths=0&search_price_min=&search_price_max=",
                "property_type" : "apartment",
                "type" : "1"
            },
            {
                "url" : "https://enrichrealty.com.au/property-search/?sort=newest&search_status=10&search_address=&search_street_no=&search_street=&search_neighborhood=&search_city=&search_state=&search_zip=&search_type=12&search_beds=0&search_baths=0&search_price_min=&search_price_max=",
                "property_type" : "house",
                "type" : "2",
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type'),"item": url.get('type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get("page", 9)
  

        for item in response.xpath("//div[@class='container pxp_mobile_fullwidth']/div/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item,meta={"property_type": response.meta.get("property_type")})

        
        if response.meta.get("type") == "1":
            page = response.xpath("//div[@class='pagination animate fade_in play']/a[last()]/text()").extract_first()
            for i in range(1,len(page)+1):
                url = f"https://www.expertissimmo.eu/fr/biens-a-louer.php?p={i}&typeFilter=1"
                yield Request(response.urljoin(item), callback=self.parse)

        if response.meta.get("type") == "2":
            page = response.xpath("//div[@class='pagination animate fade_in play']/a[last()]/text()").extract_first()
            for i in range(1,len(page)+1):
                url = f"https://www.expertissimmo.eu/fr/biens-a-louer.php?p={i}&typeFilter=2"
                yield Request(response.urljoin(item), callback=self.parse)


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)


        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))
        externalid=response.xpath("//link[@rel='shortlink']/@href").get()
        if externalid:
            item_loader.add_value("external_id",externalid.split("p=")[-1])
        item_loader.add_xpath("title", "//title/text()")
        dontallow=response.xpath("//span[.='Leased']").get()
        if dontallow:
            return 

        room_count = response.xpath("//div[@class='row']/div[span[contains(.,'BED')]]/span[contains(.,'BED')]/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0].strip())

        bathroom_count = response.xpath("//div[@class='row']/div[span[contains(.,'BED')]]/span[contains(.,'BATH')]/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0].strip())

        images = [x.split("url(")[1].split(')')[0] for x in response.xpath("//div[@class='posts-carousel']/div/@style").getall()]
        if images:
            item_loader.add_value("images", images)


        address = response.xpath("//div[@class='col-sm-12 col-md-8']/h2/text()").extract_first()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-1])

        parking = response.xpath("//div[@class='row']/div[span[contains(.,'BED')]]/span[contains(.,'CAR')]/text()").extract_first()
        if room_count:
            item_loader.add_value("parking", True)
        item_loader.add_value("external_source", "Enrichrealty_Com_PySpider_australia")

        rent = "".join(response.xpath("//span[@class='pxp-sp-top-price']/text()").getall())
        if rent:
            if "$" in rent:
                rent = rent.split("$")[1].strip().split(" ")[0]
                if rent.isdigit():
                    item_loader.add_value("rent", int(float(rent))*4)
            else:
                if "." in rent:
                    item_loader.add_value("rent", int(float(rent))*4)
            item_loader.add_value("currency", "AUD")
        rentcheck=item_loader.get_output_value("rent")
        if not rentcheck:
            rent1="".join(response.xpath("//span[@class='pxp-sp-top-price']/span/following-sibling::text()").getall())
            if rent1:
                item_loader.add_value("rent",int(rent1.split(" ")[0])*4)

        
        desc = " ".join(response.xpath("//div[@class='pxp-single-property-section']/div/p/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        

        name=response.xpath("//div[@class='enrich-agent-name']/span/text()").get()
        if name:
            item_loader.add_value('landlord_name', name)
        email=response.xpath("//a[contains(.,'@')]/text()").get()
        if email:
            item_loader.add_value('landlord_email', email)
        # phone=response.xpath("//a[contains(.,'tel')]/text()").get()
        # if phone:
        item_loader.add_value('landlord_phone', "0451 988 869")
        furnished = response.xpath("//div[contains(@class,'text-right')]/h4/text()").get()
        if furnished and "Furnished" in furnished:
            item_loader.add_value("furnished", True)

        yield item_loader.load_item()

