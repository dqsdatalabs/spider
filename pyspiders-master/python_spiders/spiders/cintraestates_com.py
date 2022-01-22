# -*- coding: utf-8 -*-
# Author: Pavit Kaur
#Team: Sabertooth
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only,format_date,remove_white_spaces
import js2xml
import lxml
import dateparser
from scrapy import Selector 
 

class CintraestatesComSpider(scrapy.Spider):
    name = "cintraestates_com"
    allowed_domains = ["cintraestates.com"] 
    start_urls = [
        {'url':'http://www.cintraestates.com/advanced-search/?status=for-rent&type=houses','property_type':'house'},
        {'url':'http://www.cintraestates.com/advanced-search/?status=for-rent&type=bungalow','property_type':'house'},
        {'url':'http://www.cintraestates.com/advanced-search/?status=for-rent&type=flat','property_type':'apartment'},
        {'url':'http://www.cintraestates.com/advanced-search/?status=for-rent&type=room','property_type':'room'},
        {'url':'http://www.cintraestates.com/advanced-search/?status=for-rent&type=studio','property_type':'studio'},
        {'url':'http://www.cintraestates.com/advanced-search/?status=for-rent&type=apartment','property_type':'apartment'}
    ]    
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url.get('url'), callback=self.parse,
                                 meta={'property_type':url.get('property_type')})

    def parse(self, response, **kwargs):
        listings=response.xpath('//a[contains(@href,"/property/")]/@href').extract()
        listings=set(listings)
        for property_url in listings:
            yield scrapy.Request(url=property_url,
                                callback=self.get_property_details,
                                meta={'request_url':property_url,'property_type':response.meta.get('property_type')})

        next_page_url=response.xpath('//a[@class="next page-numbers"]/@href').get()
        if next_page_url:
            yield response.follow(
                url=next_page_url,
                callback=self.parse,
                meta={'property_type':response.meta.get('property_type')})
                        
    def get_property_details(self,response):
        item_loader = ListingLoader(response=response)
 
        property_type=response.meta.get('property_type')
        item_loader.add_value('property_type',property_type)
        item_loader.add_xpath('external_id','//p[text()="Property ID"]/preceding-sibling::p/text()')

        item_loader.add_xpath('title','//div[@class="property-heading"]/h4/text()')

        description=response.xpath('//div[@class="ere-property-element"]/p/text()').extract()
        if description:
            description=" ".join(description)
            description=remove_white_spaces(description)
            item_loader.add_value('description',description)

            description=description.replace('-'," ").replace('.',' ').replace(':',' ')
            description=remove_white_spaces(description)
 
            epc_stringList=description.split("Rating")
            if len(epc_stringList)>1: 
                epc=epc_stringList[1].strip().split()[0]
                item_loader.add_value('energy_label',epc)             

        item_loader.add_xpath('address','//div[@class="property-address"]/span/text()')
        addresscehck=item_loader.get_output_value("address")
        if not addresscehck:
            address1="-".join(response.xpath("//div[@class='ere-property-element']/ul//li//span//text()").getall())
            if address1:
                item_loader.add_value("address",address1)
        item_loader.add_value('city','Reading')
        
        zipcode=response.xpath('//strong[contains(text(),"Postal code")]/following-sibling::span/text()').get()
        if zipcode:
            if len(zipcode)<10:
                item_loader.add_value('zipcode',zipcode)
            if len(zipcode)>10:
                item_loader.add_value('zipcode','RG1 TVP')
 
 
        javascript = response.xpath('.//script[contains(text(),"lat")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            latitude=xml_selector.xpath('.//var[@name="lat"]//text()').extract_first()
            longitude=xml_selector.xpath('.//var[@name="lng"]//text()').extract_first()
            item_loader.add_value('latitude',latitude)
            item_loader.add_value('longitude',longitude)

        room_count=response.xpath('//strong[contains(text(),"Bedroom")]/following-sibling::span/text()').extract_first()
        if room_count:
            item_loader.add_value('room_count',room_count)
        else:
            room_count = response.xpath("//li[contains(., 'Room')]/span/text()").get()
            if room_count:
                item_loader.add_value('room_count',room_count)
        
        bathroom_count=response.xpath('//strong[contains(text(),"Bathroom")]/following-sibling::span/text()').extract_first()
        if bathroom_count:
            bathroom_count=bathroom_count.strip() 
            item_loader.add_value('bathroom_count',bathroom_count)

        furnished=response.xpath('//strong[contains(text(),"Furnished")]/following-sibling::span/text()').extract_first()
        if furnished:
            furnished=furnished.strip() 
            if furnished.lower()=='furnished':
                item_loader.add_value('furnished',True)
            # http://www.cintraestates.com/property/spacious-2-double-bedroom-flat-walking-distance-to-oracle-town-stations/
            elif furnished.lower()=='unfurnished':
                item_loader.add_value('furnished',False)

        available_date=response.xpath('//strong[contains(text(),"Available date")]/following-sibling::span/text()').extract_first()
        if available_date:
            if "now" not in available_date.lower():
                if len(available_date.split('/')[-1])==2:
                    available_date="/".join(available_date.split('/')[0:2])+"/20"+available_date.split('/')[-1]
                available_date=format_date(available_date)
                item_loader.add_value('available_date',available_date)
        availabledatecheck=item_loader.get_output_value("available_date")
        if not availabledatecheck:
            available_datee=response.xpath('//div[@class="ere-property-element"]/p/text()').getall()
            for i in available_datee:
                if "available" in i.lower():
                    date=i.split("Available")[-1].strip()
                    if date:
                        date2 =  date.strip()
                        date_parsed = dateparser.parse(
                            date2, date_formats=["%m-%d-%Y"]
                        )
                        date3 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date3)




        rent_string=response.xpath('//strong[contains(text(),"Price")]/following-sibling::span/text()').extract_first()
        if rent_string:
            rent_string=rent_string.strip()
            item_loader.add_value('rent_string',rent_string)

        images=response.xpath('//div[contains(@class,"property-gallery-item")]//a/@href').extract()
        if images:
            images=list(set(images))
            item_loader.add_value('images',images)

        item_loader.add_value('landlord_name',"Cintra Estates")
        item_loader.add_value('landlord_phone','01189 311 211')
        item_loader.add_value('landlord_email','info@cintraestates.com')
        item_loader.add_value("external_source", "Cintraestates_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value("external_link",response.meta.get("request_url"))

        self.position+=1
        item_loader.add_value("position",self.position)
        yield item_loader.load_item()
