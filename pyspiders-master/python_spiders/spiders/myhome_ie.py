# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import itemloaders
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
import dateparser
from python_spiders.loaders import ListingLoader
import json
class MySpider(Spider): 
  name = 'myhome_ie'
  execution_type='testing'
  country='united_kingdom'
  locale='en'
  thousand_separator = ','
  scale_separator = '.'
  def start_requests(self):
    start_urls = [
      {
          "url": [
              "https://www.myhome.ie/rentals/ireland/apartment-to-rent",
          ],
          "property_type": "apartment"
      },
    {
        "url": [
            "https://www.myhome.ie/rentals/ireland/studio-to-rent"
        ],
        "property_type": "studio"
        },
      {
        "url": [
            "https://www.myhome.ie/rentals/ireland/house-to-rent"
        ],
        "property_type": "house"
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
    page = response.meta.get('page', 2)       
    seen = False
    for item in response.xpath("//div[@class='ng-star-inserted']/div//div[contains(@class,'PropertyListingCard__PropertyInfo')]"):
        follow_url = response.urljoin(item.xpath(".//a/@href").get())
        yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        seen = True
    if page==2 or seen:
        url = f"https://www.myhome.ie/rentals/ireland/house-to-rent?page={page}"
        yield Request(url, callback=self.parse, meta={"page": page+1,"property_type": response.meta.get('property_type')})
  # 2. SCRAPING level 2
  def populate_item(self, response):
    item_loader = ListingLoader(response=response)

    print("-----------", response.meta.get('property_type'))
    item_loader.add_value("external_link", response.url)
    item_loader.add_value("property_type", response.meta.get('property_type'))


    item_loader.add_value("external_source", "Myhome_PySpider_united_kingdom")  
    item_loader.add_xpath("title", "//title/text()")     
    item_loader.add_value("external_id", response.url.split("/")[-1])  

    address = "".join(response.xpath("//h1[@class='PropertyBrochure__Address']/text()").getall())
    if address:
        item_loader.add_value("address", address.strip())  
        item_loader.add_value("zipcode", address.split(",")[-1].strip())  

    room_count = "".join(response.xpath("//div[@class='PropertyInfoStrip']/span[contains(@class,'PropertyInfoStrip__Detail')]/text()[contains(.,'bed')]").extract())
    if room_count:
     item_loader.add_value("room_count", room_count.strip().split(" ")[0].strip()) 

    bathroom = "".join(response.xpath("//div[@class='PropertyInfoStrip']/span[contains(@class,'PropertyInfoStrip__Detail')]/text()[contains(.,'bath')]").extract())
    if bathroom:
     item_loader.add_value("bathroom_count", bathroom.strip().split(" ")[0].strip())


    available_date=response.xpath("//div[@class='PropertyInfoStrip']/span[contains(@class,'PropertyInfoStrip__Detail')]/text()[contains(.,'on')]").get()

    if available_date:
        date2 =  available_date.split("on")[1].strip()
        date_parsed = dateparser.parse(
            date2, date_formats=["%m-%d-%Y"]
        )
        date3 = date_parsed.strftime("%Y-%m-%d")
        item_loader.add_value("available_date", date3)
    rent = " ".join(response.xpath("//div[@class='PropertyBrochure__Price']/text()").getall())
    if rent:
     price = rent.split("/")[0].replace(",","").replace("€","").strip()
   
    if "POA" in price:
        return
    else:
        item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        

    deposit = " ".join(response.xpath("//span[@class='RentalPropertyInfoStrip__Detail ng-star-inserted'][2]/text()").getall())
    if deposit:
     item_loader.add_value("deposit", deposit.strip().replace("€",""))

    description = " ".join(response.xpath("//div[@class='mb-5']/section/text()").getall())
    if description:
        item_loader.add_value("description", description.strip())
    dontallow=item_loader.get_output_value("description") 
    if dontallow and "agricultural lands" in dontallow:
        return 

    images = [x for x in response.xpath("//img[@class='SimilarPropertiesCarousel__HiddenImage']//@src").getall()]
    if images: 
        item_loader.add_value("images", images) 
    

    item_loader.add_xpath("landlord_name", "normalize-space(//div[@class='GroupInfoCard__Agent mb-3']/div/a/text())")
    tel = "".join(response.xpath("//div[@class='GroupInfoCard__Agent mb-3']/div/div/text() | //div[@class='GroupInfoCard__Details GroupInfoCard__Details--dark ng-star-inserted']/span/text()").getall())
    if tel:
     item_loader.add_value("landlord_phone", tel.split(":")[-1])

    furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished')]/text() | //div[contains(.,'Furnished')]/text()").get()
    if furnished:
        if "unfurnished" in furnished.lower():
            item_loader.add_value("furnished", False)  
        else:
            item_loader.add_value("furnished", True)
    city=response.xpath("//title//text()").get()
    if city:
        item_loader.add_value("city",city.split("-")[0].split(",")[-1].strip())
    
    javascript = response.xpath("//script[contains(.,'latitude')]/text()").extract_first()
    if javascript:        
        latitude = javascript.split("latitude&q;:")[1].split("}")[0]
        longitude = javascript.split("longitude&q;:")[1].split(",")[0].split(")")[0]
        if latitude and longitude:
            item_loader.add_value('latitude', latitude)
            item_loader.add_value('longitude', longitude)
    washing_machine = response.xpath("//div[@class='FeatureList']/div[contains(.,'Washing Machine')]//text()").get()
    if washing_machine:
        item_loader.add_value("washing_machine", True)  
    parking = response.xpath("//div[@class='FeatureList']/div[contains(.,'Parking')]//text()").get()
    if parking:
        item_loader.add_value("parking", True) 

    terrace = response.xpath("//div[@class='PropertyInfoStrip']/span[contains(@class,'PropertyInfoStrip__Detail')]/text()[contains(.,'Terraced')]").get()
    if terrace:
        item_loader.add_value("terrace", True) 
    imagescheck=item_loader.get_output_value("images")
    if not imagescheck:
     id=item_loader.get_output_value("external_id")
     urlimage=f"https://api.myhome.ie/brochure/{id}?ApiKey=4284149e-13da-4f12-aed7-0d644a0b7adb&CorrelationId=7977b6bf-55a2-4946-8ec4-2450a4a5a7ee&format=json"
     yield Request(urlimage, callback=self.image,meta={"item_loader":item_loader})
    else:
     yield item_loader.load_item()
  

      
      
  def image(self,response):
    if json.loads(response.body)["Brochure"]["Property"]:
        item_loader=response.meta.get("item_loader")
        images=json.loads(response.body)["Brochure"]["Property"]["Photos"]
        try:
            if json.loads(response.body)["Brochure"]["Property"]["BrochureContactDetails"]["Phone"]:
                phone=json.loads(response.body)["Brochure"]["Property"]["BrochureContactDetails"]["Phone"]
                item_loader.add_value("landlord_phone",phone)
        except:
            pass
        name=json.loads(response.body)["Brochure"]["Property"]["BrochureContactDetails"]["FirstName"]
        if images:
            item_loader.add_value("images",images) 
        if name:
            item_loader.add_value("landlord_name",name)
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "garagebox " in p_type_string.lower():
        return None
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"    
    else:
        return None