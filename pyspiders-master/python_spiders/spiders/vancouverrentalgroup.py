import scrapy
from python_spiders.loaders import ListingLoader
from datetime import datetime
import dateutil.parser
import re

class VancouverrentalgroupSpider(scrapy.Spider):
    name = 'vancouverrentalgroup'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    
    allowed_domains = ['vancouverrentalgroup.ca']
    start_urls = ['https://www.vancouverrentalgroup.ca/listings']
    
    
    def parse(self, response):
        for item in response.css("div.container-fluid .listing_container .lst_wrapper a::attr(href)").getall():
            yield scrapy.Request(url=item, callback=self.parse_item)
            

    def parse_item(self, response):
        
        
        title           = response.css("div.col-md-offset-1 h1::text").get()
        address         = response.css("div.col-md-offset-1 h1::text").get()
        city            = response.css("div.col-md-offset-1 h2::text").get()
        rent            = response.css("div.row.padding_box_small h3::text").get()
        images          = response.css("div.hide_below_tablet img::attr(src)").getall()
        property_type   = response.xpath('//table/tbody/tr/td[contains(text(), "Type")]/following-sibling::td/text()').get()
        room_counts     = response.xpath('//table/tbody/tr/td[contains(text(), "Bedrooms")]/following-sibling::td/text()').get()
        bathroom_counts = response.xpath('//table/tbody/tr/td[contains(text(), "Bathrooms")]/following-sibling::td/text()').get()
        square_meters   = response.xpath('//table/tbody/tr/td[contains(text(), "Square Feet")]/following-sibling::td/text()').get()
        if not square_meters:
            square_meters = int(float(response.css("div.col-sm-8.col-sm-offset-2 p::text").re(r'Size:\s\w*')[0].replace("Size:","").strip())*0.3048)
        else:
            square_meters   = make_squares(square_meters)
        
        val = response.css("div.col-sm-8.col-sm-offset-2 p::text")
        if val.getall()[-1].strip() == "":
            try:
                if val.getall()[-2].strip() == "":
                    val = response.css("div.vk_sh.vk_bk::text")
            except:
                val = response.css("div.vk_sh.vk_bk::text")
        landlord_name, landlord_phone = make_info(val)
            
        parking         = response.xpath('//table/tbody/tr/td[contains(text(), "Parking")]/following-sibling::td/text()').get()
        balcony         = response.xpath('//table/tbody/tr/td[contains(text(), "Balcony/Patio")]/following-sibling::td/text()').get()
        furnished       = response.xpath('//table/tbody/tr/td[contains(text(), "Furnished")]/following-sibling::td/text()').get()
        pets_allowed    = make_pets(response.css("div.col-sm-8.col-sm-offset-2 p::text").re(r'Pets:.*'))
        washing_machine = response.xpath('//table/tbody/tr/td[contains(text(), "Washer/Dryer")]/following-sibling::td/text()').get()
        washer          = response.xpath('//table/tbody/tr/td[contains(text(), "Dishwasher")]/following-sibling::td/text()').get()
        description     = make_description(response.css("div.detail_tab div div p::text").getall())
        available_date  = make_date(response.css("div.col-sm-8.col-sm-offset-2 p::text").re(r'Available:.*'))
        
        
        images          = make_images(images)
        property_type   = make_type(property_type)
        rent, currency  = make_rent_currency(rent)
        deposit         = make_deposit(response.css("div.col-sm-8.col-sm-offset-2 p::text").re(r'Deposits:.*'), rent)
        

        
    
        for items in response.css("div.container-fluid.nopad"):
            item = ListingLoader(response=response)
            
            item.add_value("external_source"        ,self.external_source)
            item.add_value("external_link"          ,response.url)
            item.add_value("title"                  ,title)
            item.add_value("available_date"         ,available_date)
            item.add_value("address"                ,address)
            item.add_value("city"                   ,city)
            item.add_value("rent"                   ,rent)
            item.add_value("images"                 ,images)
            item.add_value("property_type"          ,property_type)
            item.add_value("room_count"             ,room_counts)
            item.add_value("bathroom_count"         ,bathroom_counts)
            item.add_value("square_meters"          ,int(int(square_meters)*10.764))
            item.add_value("parking"                ,parking != "No")
            item.add_value("balcony"                ,balcony != "No")
            item.add_value("furnished"              ,furnished != "Unfurnished")
            item.add_value("pets_allowed"           ,pets_allowed)
            item.add_value("dishwasher"             ,washer == "Dishwasher")
            item.add_value("washing_machine"        ,washing_machine != "No Laundry")
            item.add_value("description"            ,description)
            item.add_value("currency"               ,currency)
            item.add_value("deposit"                ,deposit)
            item.add_value("landlord_name"          ,landlord_name)
            item.add_value("landlord_phone"         ,landlord_phone)
            
            yield item.load_item()
    

def make_images(images):
    newImages = []
    for image in images:
        
        index = image.find("thumb")
        im  = image[:index]
        age = image[index+5:]
        image = im + "web" + age
        newImages.append(image)
    return newImages

def make_type(property_type):
    if property_type in ['Condo', 'Listing', 'Loft']:
        return 'apartment'
    return property_type

def make_rent_currency(rent):
    currency = ''
    if rent.find("$") > -1:
        currency = "CAD"
        rent = rent.replace("$", "").replace(",","")
    
    return int(float(rent[:rent.find("/")])) ,currency


def make_squares(square_meters):
    if not square_meters:
        return "not found"
    sq = ''
    for c in square_meters:
        if c in '1234567890':
            sq += c
    return int(float(sq)*0.0929)


def make_deposit(word, rent):
    if word:
        word = word[0].replace("Deposits: ","")
        if word.lower() in ["half months rent", "half month rent"]:
            return round(rent/2)
    return word



def make_date(available_date):
    if available_date:
        available_date = available_date[0].replace("Available:", "").strip()

        if available_date == "Now!!":
            available_date = datetime.now()
        else:   
            available_date = dateutil.parser.parse(available_date)

        return available_date.strftime("%Y-%m-%d")
    return available_date

def make_pets(pets):
   
    if not pets :
        return pets
    pets = pets[0].replace("Pets:","").strip()
    if pets.lower() in ['absolutely no pets', 'no pets', 'not allowed', 'prefer no pets']:
        return False
    return True


def make_description(description):
    description = " ".join(description)
    newDesc = ''
    for i in description:
        if i == "_":
            continue
        else:
            newDesc+=i
    if "Vancouver Rental Group" in newDesc:
        newDesc =  newDesc.replace("Seva Roberts", "").strip().replace("Vancouver Rental Group", "").strip().replace("Re/Max Crest Realty","").strip().replace("Remax Crest Realty","")
    return newDesc




def make_info(info):
    try:    
        if info.getall()[-1].strip() !="":
            i = info.getall()[-1]
        else:
            i = info.getall()[-2]
    except:
        pass
    try:
        
        name = re.findall("[^.]\s([A-Z]\w+)",i)[0]
        if name == "Crest":
            return re_extract(info)
        if "Please" in name:
            name = name.replace("Please", "")
            
        try:    
            phone = re.findall("[0-9]+-[0-9]+-[0-9]+",i)
        except:
            phone=""
        return name,phone 
    except:
        return "", ""
        
def re_extract(info):
    i = info.re(r'For.*')[0]
    name = re.findall("[A-Z]\w+",i)[-1]
    phone = re.findall("[0-9]+-[0-9]+-[0-9]+",i)[0]
    return name, phone
    
    
    

