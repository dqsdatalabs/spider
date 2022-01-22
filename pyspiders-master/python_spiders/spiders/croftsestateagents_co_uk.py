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
    name = 'croftsestateagents_co_uk'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'   
  
    def start_requests(self):
        start_urls = [
            {"type" : "3", "property_type" : "house"},
            {"type" : "4", "property_type" : "house"},
            {"type" : "8", "property_type" : "apartment"},
            {"type" : "9", "property_type" : "apartment"},
            {"type" : "5", "property_type" : "apartment"},
            {"type" : "6", "property_type" : "studio"},
        ]
        for url in start_urls:

            formdata = {
                "filter_cat": "2",
                "tx_placename": "",
                "filter_rad": "5",
                "eapow-qsmod-types": url.get("type"),
                "selectItemeapow-qsmod-types": url.get("type"),
                "filter_keyword": "",
                "filter_beds": "",
                "filter_price_low": "",
                "filter_price_high": "",
                "commit": "",
                "filter_lat": "0",
                "filter_lon": "0",
                "filter_location": "[object Object]",
                "filter_types": url.get("type"),
            }

            yield FormRequest(
                url="https://www.croftsestateagents.co.uk/properties?eapowquicksearch=1&limitstart=0",
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={'property_type': url.get('property_type')}
            )

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 10)
        seen = False

        for item in response.xpath("//div[@id='smallProps']/div[contains(@class,'fluid')]"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'Read more')]/@href").get())
            let_agreed = item.xpath(".//img[@alt='Under Offer']").get()
            seen = True
            if not let_agreed: yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        if page == 10 or seen:
            p_url = f"https://www.croftsestateagents.co.uk/properties?eapowquicksearch=1&limitstart={page}"
            yield Request(
                url=p_url,
                callback=self.parse,
                dont_filter=True,
                meta={'property_type': response.meta.get('property_type'), "page": page + 10}
            )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        if response.url == "https://www.croftsestateagents.co.uk/properties":
            return
        item_loader.add_value("external_source", "Croftsestateagents_Co_PySpider_united_kingdom")
        title = "".join(response.xpath("//h1/text()").getall())
        if title:
            item_loader.add_value("title", title.strip())
        external_id = response.xpath("//div[b[.='Ref #']]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[-1].strip())
        item_loader.add_xpath("room_count","//i[@class='flaticon-bed']/following-sibling::strong[1]/text()")     
        item_loader.add_xpath("bathroom_count","//i[@class='flaticon-bath']/following-sibling::strong[1]/text()")
        rent = response.xpath("//h1/small[@class='eapow-detail-price']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.split("Deposit")[0])
   
        deposit = response.xpath("//h1/small[@class='eapow-detail-price']/text()[contains(.,'Deposit ')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("Deposit")[-1])
        address = " ".join(response.xpath("//div[contains(@class,'eapow-mainaddress')]//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
        city_zip = response.xpath("//div[contains(@class,'eapow-mainaddress')]/address/text()").get()
        if city_zip:
            item_loader.add_value("city", " ".join(city_zip.strip().split(" ")[:-2]))
            item_loader.add_value("zipcode", " ".join(city_zip.strip().split(" ")[-2:]))
      
        parking = response.xpath("//li[contains(.,'parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
    
        latlng = response.xpath("//script[contains(.,'lat:') and contains(.,'lon:')]/text()").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split('lat: "')[1].split('"')[0].strip())
            item_loader.add_value("longitude", latlng.split('lon: "')[1].split('"')[0].strip())
        description = " ".join(response.xpath("//div[contains(@class,'eapow-desc-wrapper')]/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
   
        images = [x for x in response.xpath("//div[@id='eapowgalleryplug']//a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [x for x in response.xpath("//div[@id='eapowfloorplanplug']//a/img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        item_loader.add_xpath("landlord_name", "//div[@id='DetailsBox']/div[@class='row-fluid']//a[1]/b/text()")
        item_loader.add_xpath("landlord_phone", "//div[@id='DetailsBox']//div[contains(@class,'sidecol-phone')]/text()")
        item_loader.add_value("landlord_email", "info@croftsestateagents.co.uk")
        yield item_loader.load_item()
