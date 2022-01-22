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
import dateparser

class MySpider(Spider):
    name = 'lejebolig_syddan_dk'
    execution_type = 'testing'
    country = 'denmark'
    locale ='da'
    external_source="Lejebolig_Syddan_PySpider_denmark"
    start_urls = ['http://lejebolig.syddan.dk/HousingAreaRentalcases/Index/?menuId=12630&area=108&rentalcaseMenuId=12631'] # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='col-xs-12 thumbnail-rental']/@onclick").re(r"'(.+)'"):          
          follow_url = f'http://lejebolig.syddan.dk{item}'            
          yield Request(follow_url, callback=self.populate_item)


    # 2. SCRAPING level 2
    def populate_item(self, response):
      item_loader = ListingLoader(response=response)

      item_loader.add_value("external_link", response.url)
      item_loader.add_value("external_source", self.external_source)
      item_loader.add_value("external_id", response.url.split("rentalcaseId=")[1])
      item_loader.add_xpath("title","//h2/text()")
      propertype =response.url
      if get_p_type_string(propertype):
          item_loader.add_value("property_type", get_p_type_string(propertype))
      else:
          return

      price = response.xpath("//div[@id='rentalcasecost-rent']/label/text()").get()
      if price:
            rent = price.replace(".","").split(",")[0].strip()
            item_loader.add_value("rent", rent.strip())
      item_loader.add_value("currency","DKK")
      deposit=response.xpath("//span[.='Depositum']/../label/text()").get()
      if deposit:
          deposit=deposit.replace(".","").split(",")[0].strip()
          item_loader.add_value("deposit",deposit)
      item_loader.add_xpath("address", "//div[@id='rentalcasecost-address']/label/text()")
      zipcode=item_loader.get_output_value("title")
      if zipcode:
          zipcode=re.findall("\d{4}",zipcode)
          item_loader.add_value("zipcode",zipcode)

      item_loader.add_xpath("room_count", "//div[@id='rentalcasecost-rooms']/label/text()")
      item_loader.add_xpath("city", "//div[@id='HaRentalcaseTabImagesPoints']/h4/text()")
      item_loader.add_xpath("latitude", "//div[@id='mapMarker0']/span/@data-latitude")
      item_loader.add_xpath("longitude", "//div[@id='mapMarker0']/span/@data-longitude")
      meters = response.xpath("//div[@id='rentalcasecost-size']/label/text()").extract_first()
      if meters:
              s_meters = meters.replace(",",".")
              if s_meters.isdigit():
                item_loader.add_value("square_meters",int(float(s_meters)))

      available_date="".join(response.xpath("//div[@id='rentalcasecost-vacantfrom']/label/text()").getall())
      if available_date:
              date2 =  available_date.strip()

              date_parsed = dateparser.parse(
                      date2, date_formats=["%m-%d-%Y"]
              )
              date3 = date_parsed.strftime("%Y-%m-%d")
              item_loader.add_value("available_date", date3)

      description = " ".join(response.xpath("//div[@class='rentaldescription']/div/text()").getall())
      if description:
          item_loader.add_value("description", re.sub("\s{2,}", " ", description))

      pets_allowed = " ".join(response.xpath("//div[@id='rentalcasecost-pets']/label/text()").getall())
      if pets_allowed:
          if "Nej" in pets_allowed:
              item_loader.add_value("pets_allowed", False)
          else:
              item_loader.add_value("pets_allowed", True)   


      images = [response.urljoin(x) for x in response.xpath("//div[@id='thumbnailcontainer']/a/@href").getall()]
      if images:
          item_loader.add_value("images", images)


      item_loader.add_value("landlord_name", "Andi Christensen")       
      item_loader.add_value("landlord_phone", "54781400")
      yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "townhouse" in p_type_string.lower() or "unit" in p_type_string.lower() or "housing" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
