# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import dateparser
class MySpider(Spider):
    name = 'clevermove_co_uk'
    execution_type='testing' 
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'    
    external_source='Clevermove_Co_PySpider_united_kingdom'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://clevermove.co.uk/property-search/?adv_filter_keywords&adv_filter_city&adv_filter_purpose=to-let&adv_filter_type=apartment&adv_filter_status&adv_filter_numroom&adv_filter_numbath&adv_filter_price_min=0.0&adv_filter_price_max=1000000.0&submit=Submit%20Advanced%20Search",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "http://clevermove.co.uk/property-search/?adv_filter_keywords&adv_filter_city&adv_filter_purpose=to-let&adv_filter_type=house&adv_filter_status&adv_filter_numroom&adv_filter_numbath&adv_filter_price_min=0.0&adv_filter_price_max=1000000.0&submit=Submit%20Advanced%20Search",
            
                ],
                "property_type": "house"
            },
	        {
                "url": [
                    "http://clevermove.co.uk/property-search/?adv_filter_keywords&adv_filter_city&adv_filter_purpose=to-let&adv_filter_type=room&adv_filter_status&adv_filter_numroom&adv_filter_numbath&adv_filter_price_min=0.0&adv_filter_price_max=1000000.0&submit=Submit%20Advanced%20Search",
            
                ],
                "property_type": "room"
            }
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
        for url in response.xpath("//div[@class='nvr-prop-box']//h2/a/@href").getall():  
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
  
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("external_id", "substring-after(//div[@class='prop-single-desc']//p//text()[contains(.,'Property ref:')],'Property ref:')")

        item_loader.add_xpath("title", "//h1/text()")
        address = response.xpath("//div[@class='prop-address']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-1].replace("United Kingdom","").strip())
       
        desc = " ".join(response.xpath("//div[@class='prop-single-desc']//p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
 
        item_loader.add_xpath("room_count", "//div[text()='Bedroom:']/span/text()")
        item_loader.add_xpath("bathroom_count", "//div[text()='Bathroom:']/span/text()")
        
        balcony = response.xpath("//div[text()=' Balcony']/i[@class='fa fa-check']/@class").get()
        if balcony:
            item_loader.add_value("balcony", True)
        else:
            item_loader.add_value("balcony", False)
        terrace = response.xpath("//div[text()=' Roof Terrace']/i[@class='fa fa-check']/@class").get()
        if terrace:
            item_loader.add_value("terrace", True)
        else:
            item_loader.add_value("terrace", False)
        parking = response.xpath("//div[text()=' Parking']/i[@class='fa fa-check']/@class").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            item_loader.add_value("parking", False)
        swimming_pool = response.xpath("//div[text()=' Swimming Pool']/i[@class='fa fa-check']/@class").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        else:
            item_loader.add_value("swimming_pool", False)

        furnished = response.xpath("//div[text()=' Furnished']/i[@class='fa fa-check']/@class").get()
        if furnished:
            item_loader.add_value("furnished", True)
        latitude_longitude = response.xpath("//script[contains(.,'gmaps_markers') and contains(.,'long')]/text()").get()
        if latitude_longitude:  
            item_loader.add_value("latitude", latitude_longitude.split('"lat\\":\\"')[-1].split('\\"')[0])
            item_loader.add_value("longitude", latitude_longitude.split('"long\\":\\"')[-1].split('\\"')[0])
      
        images = [x.split(":url(")[-1].split(")")[0].strip() for x in response.xpath("//div[@id='slideritems']//ul[@class='slides']/li/@style").getall()]
        if images:
            item_loader.add_value("images", images)         
        rent = response.xpath("//div[text()='Price:']/span/text()").get()
        if rent:  
            if "week" in rent:
                rent = "".join(filter(str.isnumeric, rent.split('.')[0].replace(',', '').replace('\xa0', '')))
                item_loader.add_value("rent", str(int(float(rent)*4)))
                item_loader.add_value("currency", "GBP")
            else:
                item_loader.add_value("rent_string", rent)
        available_date = response.xpath("//div[text()='Available From:']/span/text()[.!='Now']").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
     
        item_loader.add_value("landlord_name", "Clever Move")
        item_loader.add_value("landlord_phone", "020 8888 5335")
        item_loader.add_value("landlord_email", "info@clevermove.co.uk")
        yield item_loader.load_item()