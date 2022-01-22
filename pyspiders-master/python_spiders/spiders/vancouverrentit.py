import scrapy
from scrapy import Request
from ..loaders import ListingLoader

class vancouverrentit_PySpider_canadaSpider(scrapy.Spider):
    name = 'vancouverrentit'
    allowed_domains = ['vancouverrentit.com']
    start_urls = ['https://www.vancouverrentit.com/available-rentals']
    country = 'canada'
    locale = 'en-ca'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'


    def parse(self, response):
        urls = response.css("#listing-item-container > div > a::attr(href)").extract()
        for url in urls:
            url = "https://www.vancouverrentit.com" + url
            print(url)
            yield Request(url=url,
                        callback=self.parse_property)

    def parse_property(self, response):
        item_loader = ListingLoader(response=response)
        external_id = None
        try:
            external_id = response.css("#rn-mlsnumber > td:nth-child(2)::text").get().split(' ')[0]
        except:
            pass
        
        title = response.css("#body > section.listing-main-info > div > div > div.col-lg-9.col-md-9.col-sm-9.col-xs-12 > h1::text").get()
         
        description = None
        try:
            description = response.css('#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > span::text').get().strip()
        except:
            pass
        trial = None
        try:
            trial1 = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > h3:nth-child(2) > span::text").extract()[16]
            trial2 = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > h3:nth-child(2) > span::text").extract()[17]
            trial = trial1 + trial2
        except:
            pass
        if trial is not None:
            description = trial
        word = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > span:nth-child(61)::text").get()
        if word is not None:
            try:
                description = word.strip()
            except:
                pass
        word2 = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > span:nth-child(54)::text").get()
        if word2 is not None:
            try:
                description = word.strip()
            except:
                pass        
        try:
            if ": ?https://youtu.be/Wnpyg0odni4?" in description:
                description = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > span:nth-child(54)::text").get()
        except:
            pass
        try:
            if ": https://www.youtube.com/watch?v=RvKFXCvNW60" in description:
                description = response.css('#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > span:nth-child(55)::text').get()
        except:
            pass
        if description is None:
            description = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > h3 > span::text").get()
         
        city = response.css("#body > section.listing-main-info > div > div > div.col-lg-9.col-md-9.col-sm-9.col-xs-12 > h3::text").get().split(', ')[-1]
        
        zipcode = None
        try:
            zipcode = response.css("#rn-postal_code > td:nth-child(2)::text").get()
        except:
            pass

        address = response.css("#rn-address > td:nth-child(2)::text").get()
        
        latitude = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-4.col-md-4.col-sm-4.col-xs-12 > div > div.listing-views-container::attr(data-latitude)").get()
        
        longitude = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-4.col-md-4.col-sm-4.col-xs-12 > div > div.listing-views-container::attr(data-longitude)").get()
        
        check_property_type = response.css("#body > section.listing-main-info > div > div > div.col-lg-9.col-md-9.col-sm-9.col-xs-12 > h3::text").get().split('/')[0]
        property_type = check_property_type.lower()
        
        square_meters = None
        try:    
            square_meterss = response.css("#rn-sqft > td:nth-child(2)::text").get().split(' ')[0]
            square_meters = round(int(square_meterss)/10.764, 1)
        except:
            pass
        if square_meters is None:
            try:
                square_meterss = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-secondary-info-and-cta-container > div > div:nth-child(1) > ul > li:nth-child(3)::text").get().split(' ')[0]
                if ',' in square_meterss:
                    arr = square_meterss.split(',')
                    square_meterss = arr[0] + arr[1]
                square_meters = round(int(square_meterss)/10.764, 1)
            except:
                pass
        
        room_count = response.css("#rn-bedrooms > td:nth-child(2)::text").get()
        
        bathroom_count = response.css("#rn-bathrooms > td:nth-child(2)::text").get()
        
        images = response.css("#bg-fade-carousel > div > div > div > a::attr(href)").extract()
        
        external_images_count = len(images)
        
        rent = response.css("#rn-listprice > td:nth-child(2)::text").get().split('$')[-1]
        
        currency = "CAD"
        
        pets_check = response.css("#rn-pets > td:nth-child(2)::text").get()
        pets_allowed = None
        try:
            if "Yes" in pets_check or "Negotiable" in pets_check or "Cats Only" in pets_check:
                pets_allowed = True
            else:
                pets_allowed = False
        except:
            pass
        if pets_allowed is None:
            pets_check = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > span:nth-child(43)::text").get()
            try:
                if "Yes" in pets_check or "Negotiable" in pets_check or "Cats Only" in pets_check:
                    pets_allowed = True
                else:
                    pets_allowed = False
            except:
                pass
        if pets_allowed is None:
            pets_allowed = False
        
        furnished = None
        try:
            furnished_check = response.css("#rn-furnishing > td:nth-child(2)::text").get()
            furnished_check1 = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > span:nth-child(34)::text").get()
            furnished_check2 = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > span:nth-child(25)::text").get()
            furnished_check3 = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > span:nth-child(31)::text").get()
            
            if furnished_check == "No" or "No" in furnished_check1 or "No" in furnished_check2 or "No" in furnished_check3:
                furnished = False
            else:
                furnished = True
        except:
            pass
        if furnished is None:
            furnished_check4 = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > h3:nth-child(2) > span:nth-child(30)::text").get()
            if "Yes" in furnished_check4:
                furnished = True
            else:
                furnished = False
        
        floor = None
        try:
            floor_check = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > span:nth-child(10)::text").get()
            if "th" in floor_check:
                floor = floor_check.split("th")[0]
        except:
            pass

        parking = None
        try:
            parking_check = response.css("#rn-parking > td:nth-child(2)::text").get()
            if parking_check is not None:
                parking = True
        except:
            pass
        if parking is None:
            try:
                parking_check1 = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > span:nth-child(49)::text").get()
                parking_check2 = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > span:nth-child(40)::text").get()
                parking_check3 = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > span:nth-child(28)::text").get()
                parking_check4 = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > h3:nth-child(2) > span:nth-child(42)::text").get()
                if "1" or "Yes" in parking_check1 or "1" or "Yes" in parking_check2 or "1" or "Yes" in parking_check3 or "1" or "Yes" in parking_check4:
                    parking = True
                else:
                    parking = False
            except:
                pass
        
        washing_machine = None
        try:
            washing_machine_check = response.css("#rn-amenities > td:nth-child(2)::text").get()
            if "Laundry" in washing_machine_check:
                washing_machine = True
        except:
            pass
        if washing_machine is None:
            try:
                washing_machine_check1 = response.css("#body > section.content-section.section-listing-details > div > div > div.listing-main-content-container > div > div.col-lg-8.col-md-8.col-sm-8.col-xs-12 > div.listing-detail-description > span:nth-child(37)::text").get()
                if "Included" in washing_machine_check1:
                    washing_machine = True
                else:
                    washing_machine = False
            except:
                pass
        if washing_machine is None:
            washing_machine = False    
        
        landlord_name = response.css("#rn-manager > td:nth-child(2)::text").get()
        if landlord_name is None:
            landlord_name = "vancouverrentit"

        
        item_loader.add_value('external_link', response.url)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value('title', title)
        item_loader.add_value('description', description)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('address', address)
        item_loader.add_value('latitude', latitude)
        item_loader.add_value('longitude', longitude)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('square_meters', int(int(square_meters)*10.764))
        item_loader.add_value('room_count', room_count)
        item_loader.add_value('bathroom_count', bathroom_count)
        item_loader.add_value('images', images)
        item_loader.add_value('external_images_count', external_images_count)
        item_loader.add_value('rent', rent)
        item_loader.add_value('currency', currency)
        item_loader.add_value('pets_allowed', pets_allowed)
        item_loader.add_value('furnished', furnished)
        item_loader.add_value('floor', floor)
        item_loader.add_value('parking', parking)
        item_loader.add_value('washing_machine', washing_machine)
        item_loader.add_value('landlord_name', landlord_name)
        item_loader.add_value("landlord_email", "services@vancouverrentit.com‎")
        item_loader.add_value("landlord_phone", "(604)910-7368")

        yield item_loader.load_item()